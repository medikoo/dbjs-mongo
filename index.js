'use strict';

var constant          = require('es5-ext/function/constant')
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
	this.mongoDb = connect(buildUrl(data));
	this.mongoDb.done();
	this.collection = this.mongoDb.invokeAsync('collection', data.collection);
};
setPrototypeOf(MongoDriver, PersistenceDriver);

MongoDriver.prototype = Object.create(PersistenceDriver.prototype, {
	constructor: d(MongoDriver),
	_getCustom: d(function (key) {
		return this.collection.invokeAsync('find', { _id: '_' + key })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record ? record.value : getUndefined);
			});
		});
	}),
	_loadValue: d(function (id) {
		return this.collection.invokeAsync('find', { _id: id })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record
					? this._importValue(id, record.value, record.stamp) : null);
			}.bind(this));
		}.bind(this));
	}),
	_load: d(function (query) {
		return this.collection.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				return cursor.closePromised()(records.map(function (record) {
					if (record._id[0] === '=') return; // computed record
					if (record._id[0] === '_') return; // custom record
					return this._importValue(record._id, record.value, record.stamp);
				}, this).filter(Boolean));
			}.bind(this));
		}.bind(this));
	}),
	_loadObject: d(function (id) { return this._load({ _id: { $gte: id, $lt: id + '/\uffff' } }); }),
	_loadAll: d(function () { return this._load(); }),
	_storeCustom: d(function (key, value) {
		if (value === undefined) return this.collection.invokeAsync('remove', { _id: '_' + key });
		return this.collection.invokeAsync('update', { _id: '_' + key }, { value: value }, updateOpts);
	}),
	_storeEvent: d(function (event) {
		return this.collection.invokeAsync('update', { _id: event.object.__valueId__ }, {
			stamp: event.stamp,
			value: serialize(event.value)
		}, updateOpts);
	}),
	_storeEvents: d(function (events) { return deferred.map(events, this._storeEvent, this); }),
	_getComputed: d(function (id) {
		return this.collection.invokeAsync('find', { _id: '=' + id })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record || getNull);
			});
		});
	}),
	_getAllComputed: d(function (keyPath) {
		var query = { _id: { $gte: '=', $lt: '=\uffff' } }, map = create(null);
		return this.collection.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.forEach(function (record) {
					var id = record._id.slice(1)
					  , objId = id.split('/', 1)[0], localKeyPath = id.slice(objId.length + 1);
					if (localKeyPath !== keyPath) return;
					map[id] = record;
				}, this);
				return cursor.closePromised();
			}.bind(this));
		}.bind(this))(map);
	}),
	_storeComputed: d(function (id, value, stamp) {
		return this.collection.invokeAsync('update', { _id: '=' + id }, {
			stamp: stamp,
			value: value
		}, updateOpts);
	}),
	_close: d(function () { return this.mongoDb.invokeAsync('close'); })
});
