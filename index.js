'use strict';

var constant          = require('es5-ext/function/constant')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject      = require('es5-ext/object/valid-object')
  , resolveKeyPath    = require('dbjs/_setup/utils/resolve-key-path')
  , d                 = require('d')
  , deferred          = require('deferred')
  , MongoClient       = require('mongodb').MongoClient
  , MongoCursor       = require('mongodb/lib/cursor')
  , PersistenceDriver = require('dbjs-persistence/abstract')

  , create = Object.create, promisify = deferred.promisify
  , getUndefined = constant(undefined), getNull = constant(null)
  , connect = promisify(MongoClient.connect)
  , updateOpts = { upsert: true };

Object.defineProperties(MongoCursor.prototype, {
	nextPromised: d(promisify(MongoCursor.prototype.next)),
	toArrayPromised: d(promisify(MongoCursor.prototype.toArray)),
	closePromised: d(promisify(MongoCursor.prototype.close))
});

var buildUrl = function (conf) {
	var url = 'mongodb://';
	if (conf.user && conf.password) url += conf.user + ':' + conf.password + '@';
	url += (conf.host != null) ? conf.host : 'localhost';
	url += ':';
	url += (conf.port != null) ? conf.port : '27017';
	return url + '/' + conf.database;
};
var MongoDriver = module.exports = function (dbjs, data) {
	var collection;
	if (!(this instanceof MongoDriver)) return new MongoDriver(dbjs, data);
	ensureObject(data);
	ensureString(data.database);
	collection = ensureString(data.collection);
	PersistenceDriver.call(this, dbjs, data);
	this.mongoDb = connect(buildUrl(data)).aside(null, this.emitError);
	this.directDb = this.mongoDb.invokeAsync('collection', collection);
	this.computedDb = this.mongoDb.invokeAsync('collection', collection + '-computed');
	this.reducedDb = this.mongoDb.invokeAsync('collection', collection + '-reduced');
};
setPrototypeOf(MongoDriver, PersistenceDriver);

MongoDriver.prototype = Object.create(PersistenceDriver.prototype, {
	constructor: d(MongoDriver),

	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'reduced') return this._getReduced(ns + (path ? ('/' + path) : ''));
		if (cat === 'computed') return this._getComputed(path, ns);
		return this._getDirect(ns, path);
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') return this._storeReduced(ns + (path ? ('/' + path) : ''), data);
		if (cat === 'computed') return this._storeComputed(path, ns, data);
		return this._storeDirect(ns, path, data);
	}),

	// Database data
	__getDirectObject: d(function (objId, keyPaths) {
		return this._loadDirect({ _id: { $gte: objId, $lt: objId + '/\uffff' } },
			keyPaths && function (ownerId, path) { return keyPaths.has(resolveKeyPath(path)); });
	}),
	__getDirectAll: d(function () { return this._loadDirect(); }),

	// Reduced data
	__getReducedNs: d(function (ns, keyPaths) {
		var query = { _id: { $gte: ns, $lt: ns + '/\uffff' } };
		return this.reducedDb.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				var result = create(null);
				records.forEach(function (record) { result[record._id] = record; });
				return cursor.closePromised()(result);
			}.bind(this));
		}.bind(this));
	}),

	// Size tracking
	__searchDirect: d(function (callback) {
		return this.directDb.invokeAsync('find')(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.forEach(function (record) { callback(record._id, record); });
				return cursor.closePromised()(getUndefined);
			}.bind(this));
		}.bind(this));
	}),
	__searchComputed: d(function (keyPath, callback) {
		var query = { _id: { $gt: keyPath + ':', $lt: keyPath + ':\uffff' } };
		return this.computedDb.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.forEach(function (record) {
					callback(record._id.slice(record._id.lastIndexOf(':') + 1), record);
				});
				return cursor.closePromised()(getUndefined);
			});
		});
	}),

	// Storage import/export
	__exportAll: d(function (destDriver) {
		var count = 0;
		var promise = deferred(
			this.directDb.invokeAsync('find')(function (cursor) {
				return cursor.toArrayPromised()(function (records) {
					return cursor.closePromised()(deferred.map(records, function (record) {
						var index, ns, path;
						if (!(++count % 1000)) promise.emit('progress');
						index = record._id.indexOf('/');
						ns = (index === -1) ? record._id : record._id.slice(0, index);
						path = (index === -1) ? null : record._id.slice(index + 1);
						return destDriver._storeRaw('direct', ns, path, record);
					}, this));
				}.bind(this));
			}.bind(this)),
			this.computedDb.invokeAsync('find')(function (cursor) {
				return cursor.toArrayPromised()(function (records) {
					return cursor.closePromised()(deferred.map(records, function (record) {
						var index, ns, path;
						if (!(++count % 1000)) promise.emit('progress');
						index = record._id.lastIndexOf(':');
						ns = record._id.slice(0, index);
						path = record._id.slice(index + 1);
						return destDriver._storeRaw('computed', ns, path, record);
					}, this));
				}.bind(this));
			}.bind(this)),
			this.reducedDb.invokeAsync('find')(function (cursor) {
				return cursor.toArrayPromised()(function (records) {
					return cursor.closePromised()(deferred.map(records, function (record) {
						var index, ns, path;
						if (!(++count % 1000)) promise.emit('progress');
						index = record._id.indexOf('/');
						ns = (index === -1) ? record._id : record._id.slice(0, index);
						path = (index === -1) ? null : record._id.slice(index + 1);
						return destDriver._storeRaw('reduced', ns, path, record);
					}, this));
				}.bind(this));
			}.bind(this))
		);
		return promise;
	}),
	__clear: d(function () {
		return deferred(
			this.directDb.invokeAsync('deleteMany'),
			this.computedDb.invokeAsync('deleteMany'),
			this.reducedDb.invokeAsync('deleteMany')
		);
	}),

	// Connection related
	__close: d(function () { return this.mongoDb.invokeAsync('close'); }),

	// Driver specific
	_getDirect: d(function (ownerId, path) {
		return this.directDb.invokeAsync('find', { _id: ownerId + (path ? ('/' + path) : '') })(
			function (cursor) {
				return cursor.nextPromised()(function (record) {
					return cursor.closePromised()(record || getNull);
				}.bind(this));
			}.bind(this)
		);
	}),
	_getComputed: d(function (objId, keyPath) {
		return this.computedDb.invokeAsync('find', { _id: keyPath + ':' + objId })(
			function (cursor) {
				return cursor.nextPromised()(function (record) {
					return cursor.closePromised()(record || getNull);
				});
			}
		);
	}),
	_getReduced: d(function (key) {
		return this.reducedDb.invokeAsync('find', { _id: key })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record || getNull);
			});
		});
	}),
	_storeDirect: d(function (ownerId, path, data) {
		return this.directDb.invokeAsync('update', { _id: ownerId + (path ? ('/' + path) : '') },
			{ value: data.value, stamp: data.stamp }, updateOpts);
	}),
	_storeComputed: d(function (objId, keyPath, data) {
		return this.computedDb.invokeAsync('update', { _id: keyPath + ':' + objId }, {
			stamp: data.stamp,
			value: data.value
		}, updateOpts);
	}),
	_storeReduced: d(function (key, data) {
		return this.reducedDb.invokeAsync('update', { _id: key }, data, updateOpts);
	}),
	_loadDirect: d(function (query, filter) {
		return this.directDb.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				var result = create(null);
				records.forEach(function (record) {
					var index, ownerId, path;
					if (filter) {
						index = record._id.indexOf('/');
						ownerId = (index !== -1) ? record._id.slice(0, index) : record._id;
						path = (index !== -1) ? record._id.slice(index + 1) : null;
						if (!filter(ownerId, path)) return; // filtered
					}
					result[record._id] = record;
				});
				return cursor.closePromised()(result);
			}.bind(this));
		}.bind(this));
	})
});
