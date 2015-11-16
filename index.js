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
	if (!(this instanceof MongoDriver)) return new MongoDriver(dbjs, data);
	ensureObject(data);
	ensureString(data.database);
	ensureString(data.collection);
	PersistenceDriver.call(this, dbjs, data);
	this.mongoDb = connect(buildUrl(data)).aside(null, this.emitError);
	this.collection = this.mongoDb.invokeAsync('collection', data.collection);
};
setPrototypeOf(MongoDriver, PersistenceDriver);

MongoDriver.prototype = Object.create(PersistenceDriver.prototype, {
	constructor: d(MongoDriver),

	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'reduced') return this._getReduced(ns + (path ? ('/' + path) : ''));
		if (cat === 'computed') return this._getIndexedValue(path, ns);
		return this.collection.invokeAsync('find', { _id: ns + (path ? ('/' + path) : '') })(
			function (cursor) {
				return cursor.nextPromised()(function (record) {
					return cursor.closePromised()(record || getNull);
				}.bind(this));
			}.bind(this)
		);
	}),
	__getRawObject: d(function (objId, keyPaths) {
		return this._loadDirect({ _id: { $gte: objId, $lt: objId + '/\uffff' } },
			keyPaths && function (ownerId, path) { return keyPaths.has(resolveKeyPath(path)); });
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') return this._storeReduced(ns + (path ? ('/' + path) : ''), data);
		if (cat === 'computed') return this._storeIndexedValue(path, ns, data);
		return this.collection.invokeAsync('update', { _id: ns + (path ? ('/' + path) : '') },
			{ value: data.value, stamp: data.stamp }, updateOpts);
	}),

	// Database data
	__getRawAllDirect: d(function () { return this._loadDirect(); }),

	// Size tracking
	__searchDirect: d(function (callback) {
		return this.collection.invokeAsync('find')(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.forEach(function (record) {
					if (record._id[0] === '=') return; // computed record
					if (record._id[0] === '_') return; // reduced record
					callback(record._id, record);
				});
				return cursor.closePromised()(getUndefined);
			}.bind(this));
		}.bind(this));
	}),
	__searchIndex: d(function (keyPath, callback) {
		var query = { _id: { $gte: '=' + keyPath + ':', $lt: '=' + keyPath + ':\uffff' } };
		return this.collection.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.forEach(function (record) {
					callback(record._id.slice(record._id.lastIndexOf(':') + 1), record);
				});
				return cursor.closePromised()(getUndefined);
			});
		});
	}),

	// Reduced data
	__getReducedNs: d(function (ns, keyPaths) {
		var query = { _id: { $gte: '_' + ns, $lt: '_' + ns + '/\uffff' } };
		return this.collection.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				var result = create(null);
				records.forEach(function (record) { result[record._id.slice(1)] = record; });
				return cursor.closePromised()(result);
			}.bind(this));
		}.bind(this));
	}),

	// Storage import/export
	__exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.collection.invokeAsync('find')(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				return cursor.closePromised()(deferred.map(records, function (record) {
					var index, id, cat, ns, path;
					if (!(++count % 1000)) promise.emit('progress');
					if (record._id[0] === '=') {
						cat = 'computed';
						id = record._id.slice(1);
						index = id.lastIndexOf(':');
						ns = id.slice(0, index);
						path = id.slice(index + 1);
					} else {
						if (record._id[0] === '_') {
							cat = 'reduced';
							id = record._id.slice(1);
						} else {
							id = record._id;
							cat = 'direct';
						}
						index = id.indexOf('/');
						ns = (index === -1) ? id : id.slice(0, index);
						path = (index === -1) ? null : id.slice(index + 1);
					}
					return destDriver._storeRaw(cat, ns, path, record);
				}, this));
			}.bind(this));
		}.bind(this));
		return promise;
	}),
	__clear: d(function () {
		return this.collection.invokeAsync('deleteMany');
	}),

	// Connection related
	__close: d(function () {
		return this.mongoDb.invokeAsync('close');
	}),

	// Driver specific
	_getIndexedValue: d(function (objId, keyPath) {
		return this.collection.invokeAsync('find', { _id: '=' + keyPath + ':' + objId })(
			function (cursor) {
				return cursor.nextPromised()(function (record) {
					return cursor.closePromised()(record || getNull);
				});
			}
		);
	}),
	_storeIndexedValue: d(function (objId, keyPath, data) {
		return this.collection.invokeAsync('update', { _id: '=' + keyPath + ':' + objId }, {
			stamp: data.stamp,
			value: data.value
		}, updateOpts);
	}),
	_getReduced: d(function (key) {
		return this.collection.invokeAsync('find', { _id: '_' + key })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record || getNull);
			});
		});
	}),
	_storeReduced: d(function (key, data) {
		return this.collection.invokeAsync('update', { _id: '_' + key }, data, updateOpts);
	}),
	_loadDirect: d(function (query, filter) {
		return this.collection.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				var result = create(null);
				records.forEach(function (record) {
					var index, ownerId, path;
					if (record._id[0] === '=') return; // computed record
					if (record._id[0] === '_') return; // reduced record
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
