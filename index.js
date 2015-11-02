'use strict';

var flatten           = require('es5-ext/array/#/flatten')
  , constant          = require('es5-ext/function/constant')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject      = require('es5-ext/object/valid-object')
  , d                 = require('d')
  , deferred          = require('deferred')
  , MongoClient       = require('mongodb').MongoClient
  , MongoCursor       = require('mongodb/lib/cursor')
  , PersistenceDriver = require('dbjs-persistence/abstract')

  , promisify = deferred.promisify
  , getUndefined = constant(undefined)
  , getNull = constant(null)
  , connect = promisify(MongoClient.connect)
  , updateOpts = { upsert: true }
  , byStamp = function (a, b) { return a.data.stamp - b.data.stamp; };

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
	_getRaw: d(function (id) {
		var index;
		if (id[0] === '_') return this._getCustom(id.slice(1));
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			return this._getIndexedValue(id.slice(index + 1), id.slice(1, index));
		}
		return this.collection.invokeAsync('find', { _id: id })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record || getNull);
			}.bind(this));
		}.bind(this));
	}),
	_getRawObject: d(function (objId) {
		return this._load({ _id: { $gte: objId, $lt: objId + '/\uffff' } });
	}),
	_storeRaw: d(function (id, value) {
		var index;
		if (id[0] === '_') return this._storeCustom(id.slice(1), value);
		if (id[0] === '=') {
			index = id.lastIndexOf(':');
			return this._storeIndexedValue(id.slice(index + 1), id.slice(1, index), value);
		}
		return this.collection.invokeAsync('update', { _id: id },
			{ value: value.value, stamp: value.stamp }, updateOpts);
	}),

	// Database data
	_loadAll: d(function () {
		var count = 0;
		var promise = this._load().map(function (data) {
			if (!(++count % 1000)) promise.emit('progress');
			return this._importValue(data.id, data.data.value, data.data.stamp);
		}.bind(this)).invoke(flatten);
		return promise;
	}),
	_storeEvent: d(function (ownerId, targetPath, data) {
		var id = ownerId + (targetPath ? ('/' + targetPath) : '');
		return this.collection.invokeAsync('update', { _id: id }, data, updateOpts);
	}),

	// Indexed database data
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

	// Size tracking
	_searchDirect: d(function (callback) {
		return this._load().map(function (data) {
			callback(data.id, data.data);
		});
	}),
	_searchIndex: d(function (keyPath, callback) {
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

	// Custom data
	_getCustom: d(function (key) {
		return this.collection.invokeAsync('find', { _id: '_' + key })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record || getNull);
			});
		});
	}),
	_storeCustom: d(function (key, data) {
		return this.collection.invokeAsync('update', { _id: '_' + key }, data, updateOpts);
	}),

	// Storage import/export
	_exportAll: d(function (destDriver) {
		var count = 0;
		var promise = this.collection.invokeAsync('find')(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				return cursor.closePromised()(deferred.map(records, function (record) {
					if (!(++count % 1000)) promise.emit('progress');
					return destDriver._storeRaw(record._id, record);
				}, this));
			}.bind(this));
		}.bind(this));
		return promise;
	}),
	_clear: d(function () {
		return this.collection.invokeAsync('deleteMany');
	}),

	// Connection related
	_close: d(function () {
		return this.mongoDb.invokeAsync('close');
	}),

	// Driver specific
	_load: d(function (query) {
		var promise = this.collection.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				return cursor.closePromised()(records.map(function (record) {
					if (record._id[0] === '=') return; // computed record
					if (record._id[0] === '_') return; // custom record
					return { id: record._id, data: record };
				}).filter(Boolean).sort(byStamp));
			}.bind(this));
		}.bind(this));
		return promise;
	})
});
