'use strict';

var flatten           = require('es5-ext/array/#/flatten')
  , constant          = require('es5-ext/function/constant')
  , setPrototypeOf    = require('es5-ext/object/set-prototype-of')
  , ensureString      = require('es5-ext/object/validate-stringifiable-value')
  , ensureObject      = require('es5-ext/object/valid-object')
  , d                 = require('d')
  , deferred          = require('deferred')
  , serialize         = require('dbjs/_setup/serialize/value')
  , MongoClient       = require('mongodb').MongoClient
  , MongoCursor       = require('mongodb/lib/cursor')
  , PersistenceDriver = require('dbjs-persistence/abstract')

  , create = Object.create, promisify = deferred.promisify
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
				return cursor.closePromised()(record);
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
	_storeEvent: d(function (event) {
		return this.collection.invokeAsync('update', { _id: event.object.__valueId__ }, {
			stamp: event.stamp,
			value: serialize(event.value)
		}, updateOpts);
	}),
	_storeEvents: d(function (events) { return deferred.map(events, this._storeEvent, this); }),

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
	_getIndexedMap: d(function (keyPath) {
		var query = { _id: { $gte: '=' + keyPath + ':', $lt: '=' + keyPath + ':\uffff' } }
		  , map = create(null);
		return this.collection.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.forEach(function (record) {
					map[record._id.slice(record._id.lastIndexOf(':') + 1)] = record;
				}, this);
				return cursor.closePromised();
			}.bind(this));
		}.bind(this))(map);
	}),
	_storeIndexedValue: d(function (objId, keyPath, data) {
		return this.collection.invokeAsync('update', { _id: '=' + keyPath + ':' + objId }, {
			stamp: data.stamp,
			value: data.value
		}, updateOpts);
	}),

	// Custom data
	_getCustom: d(function (key) {
		return this.collection.invokeAsync('find', { _id: '_' + key })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record ? record.value : getUndefined);
			});
		});
	}),
	_storeCustom: d(function (key, value) {
		if (value === undefined) return this.collection.invokeAsync('remove', { _id: '_' + key });
		return this.collection.invokeAsync('update', { _id: '_' + key }, { value: value }, updateOpts);
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
