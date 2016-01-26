'use strict';

var assign           = require('es5-ext/object/assign')
  , constant         = require('es5-ext/function/constant')
  , setPrototypeOf   = require('es5-ext/object/set-prototype-of')
  , serializeValue   = require('dbjs/_setup/serialize/value')
  , unserializeValue = require('dbjs/_setup/unserialize/value')
  , d                = require('d')
  , lazy             = require('d/lazy')
  , deferred         = require('deferred')
  , ReducedStorage   = require('dbjs-persistence/reduced-storage')

  , create = Object.create
  , getNull = constant(null)
  , isUnserializable = RegExp.prototype.test.bind(/[01234]/)
  , updateOpts = { upsert: true };

var MongoReducedStorage = module.exports = function (driver) {
	if (!(this instanceof MongoReducedStorage)) return new MongoReducedStorage(driver);
	ReducedStorage.call(this, driver);
};
setPrototypeOf(MongoReducedStorage, ReducedStorage);

MongoReducedStorage.prototype = Object.create(ReducedStorage.prototype, assign({
	constructor: d(MongoReducedStorage),

	// Any data
	__get: d(function (ns, path) { return this._get_(ns + (path ? ('/' + path) : '')); }),
	__store: d(function (ns, path, data) { return this._store_(ns, path, data); }),
	__getObject: d(function (ns, keyPaths) {
		return this.reducedDb.invokeAsync('find', { ns: ns })(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				var result = create(null);
				records.forEach(function (record) {
					result[record._id] = {
						value: record.unserialized ? serializeValue(record.value) : record.value,
						stamp: record.stamp
					};
				});
				return cursor.closePromised()(result);
			}.bind(this));
		}.bind(this));
	}),

	// Storage import/export
	__exportAll: d(function (destStorage) {
		var count = 0;
		var promise = deferred(
			this.reducedDb.invokeAsync('find')(function (cursor) {
				return cursor.toArrayPromised()(function (records) {
					return cursor.closePromised()(deferred.map(records, function (record) {
						if (!(++count % 1000)) promise.emit('progress');
						return destStorage._storeRaw(record.ns, record.path, {
							value: record.unserialized ? serializeValue(record.value) : record.value,
							stamp: record.stamp
						});
					}, this));
				}.bind(this));
			}.bind(this))
		);
		return promise;
	}),
	__clear: d(function () {
		return deferred(this.hasOwnProperty('reducedDb') && this.reducedDb.invokeAsync('drop'));
	}),
	__drop: d(function () { return this.__clear(); }),

	// Connection related
	__close: d(function () { return deferred(undefined); }),

	// Driver specific
	_get_: d(function (key) {
		return this.reducedDb.invokeAsync('find', { _id: key })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record ? {
					value: record.unserialized ? serializeValue(record.value) : record.value,
					stamp: record.stamp
				} : getNull);
			});
		});
	}),
	_store_: d(function (ns, path, data) {
		var unserialized = Boolean(data.value && isUnserializable(data.value[0]));
		var record = {
			ns: ns,
			value: unserialized ? unserializeValue(data.value) : data.value,
			stamp: data.stamp
		};
		if (path) record.path = path;
		if (unserialized) record.unserialized = true;
		return this.reducedDb.invokeAsync('update', { _id: ns + (path ? ('/' + path) : '') },
			record, updateOpts);
	})
}, lazy({
	reducedDb: d(function () { return this.driver.mongoDb.invokeAsync('collection', '_reduced'); })
})));
