'use strict';

var assign              = require('es5-ext/object/assign')
  , constant            = require('es5-ext/function/constant')
  , setPrototypeOf      = require('es5-ext/object/set-prototype-of')
  , serializeValue      = require('dbjs/_setup/serialize/value')
  , unserializeValue    = require('dbjs/_setup/unserialize/value')
  , resolveKeyPath      = require('dbjs/_setup/utils/resolve-key-path')
  , d                   = require('d')
  , lazy                = require('d/lazy')
  , deferred            = require('deferred')
  , Storage             = require('dbjs-persistence/storage')
  , resolveValue        = require('dbjs-persistence/lib/resolve-direct-value')
  , filterComputedValue = require('dbjs-persistence/lib/filter-computed-value')

  , create = Object.create
  , getNull = constant(null)
  , isUnserializable = RegExp.prototype.test.bind(/[01234]/)
  , updateOpts = { upsert: true };

var MongoStorage = module.exports = function (driver, name/*, options*/) {
	if (!(this instanceof MongoStorage)) return new MongoStorage(driver, name, arguments[2]);
	Storage.call(this, driver, name, arguments[2]);
};
setPrototypeOf(MongoStorage, Storage);

MongoStorage.prototype = Object.create(Storage.prototype, assign({
	constructor: d(MongoStorage),

	// Any data
	__getRaw: d(function (cat, ns, path) {
		if (cat === 'reduced') return this._getReduced_(ns + (path ? ('/' + path) : ''));
		if (cat === 'computed') return this._getComputed_(path, ns);
		return this._getDirect_(ns, path);
	}),
	__storeRaw: d(function (cat, ns, path, data) {
		if (cat === 'reduced') return this._storeReduced_(ns, path, data);
		if (cat === 'computed') return this._storeComputed_(path, ns, data);
		return this._storeDirect_(ns, path, data);
	}),

	// Direct data
	__getObject: d(function (ownerId, objectPath, keyPaths) {
		var query;
		if (objectPath) {
			query = {
				_id: { $gte: ownerId + '/' + objectPath, $lt:  ownerId + '/' + objectPath + '/\uffff' }
			};
		} else {
			query = { ownerId: ownerId };
		}
		return this._loadDirect_(query, keyPaths && function (ownerId, path) {
			if (!path) return true;
			return keyPaths.has(resolveKeyPath(ownerId + '/' + path));
		});
	}),
	__getAllObjectIds: d(function () {
		return this.directDb.invokeAsync('find')(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				var data = create(null);
				records.forEach(function (record) {
					if (!record.keyPath) data[record.ownerId] = record;
				});
				return cursor.closePromised()(data);
			});
		}.bind(this));
	}),
	__getAll: d(function () { return this._loadDirect_(); }),

	// Reduced data
	__getReducedObject: d(function (ns, keyPaths) {
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

	__search: d(function (keyPath, value, callback) {
		return this.directDb.invokeAsync('find')(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.some(function (record) {
					var recordValue, resolvedValue;
					if (keyPath !== undefined) {
						if (!keyPath) {
							if (record.keyPath) return;
						} else if (keyPath !== record.keyPath) {
							return;
						}
					}
					recordValue = record.unserialized ? serializeValue(record.value) : record.value;
					if (value != null) {
						resolvedValue = resolveValue(record.ownerId, record.path, recordValue);
						if (value !== resolvedValue) return;
					}
					return callback(record._id, {
						value: recordValue,
						stamp: record.stamp
					});
				});
				return cursor.closePromised()(Function.prototype);
			}.bind(this));
		}.bind(this));
	}),
	__searchComputed: d(function (keyPath, value, callback) {
		var query;
		if (keyPath) query = { keyPath: keyPath };
		return this.computedDb.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				records.some(function (record) {
					var recordValue = record.unserialized ? serializeValue(record.value) : record.value;
					if ((value != null) && !filterComputedValue(value, recordValue)) return;
					return callback(record.ownerId, {
						value: recordValue,
						stamp: record.stamp
					});
				});
				return cursor.closePromised()(Function.prototype);
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
						if (!(++count % 1000)) promise.emit('progress');
						return destDriver._storeRaw('direct', record.ownerId, record.path, {
							value: record.unserialized ? serializeValue(record.value) : record.value,
							stamp: record.stamp
						});
					}, this));
				}.bind(this));
			}.bind(this)),
			this.computedDb.invokeAsync('find')(function (cursor) {
				return cursor.toArrayPromised()(function (records) {
					return cursor.closePromised()(deferred.map(records, function (record) {
						if (!(++count % 1000)) promise.emit('progress');
						return destDriver._storeRaw('computed', record.keyPath, record.ownerId, {
							value: record.unserialized ? serializeValue(record.value) : record.value,
							stamp: record.stamp
						});
					}, this));
				}.bind(this));
			}.bind(this)),
			this.reducedDb.invokeAsync('find')(function (cursor) {
				return cursor.toArrayPromised()(function (records) {
					return cursor.closePromised()(deferred.map(records, function (record) {
						if (!(++count % 1000)) promise.emit('progress');
						return destDriver._storeRaw('reduced', record.ns, record.path, {
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
		return deferred(
			this.hasOwnProperty('directDb') && this.directDb.invokeAsync('drop'),
			this.hasOwnProperty('computedDb') && this.computedDb.invokeAsync('drop'),
			this.hasOwnProperty('reducedDb') && this.reducedDb.invokeAsync('drop')
		);
	}),
	__drop: d(function () { return this.__clear(); }),

	// Connection related
	__close: d(function () { return deferred(undefined); }),

	// Driver specific
	_getDirect_: d(function (ownerId, path) {
		return this.directDb.invokeAsync('find', { _id: ownerId + (path ? ('/' + path) : '') })(
			function (cursor) {
				return cursor.nextPromised()(function (record) {
					return cursor.closePromised()(record ? {
						value: record.unserialized ? serializeValue(record.value) : record.value,
						stamp: record.stamp
					} : getNull);
				}.bind(this));
			}.bind(this)
		);
	}),
	_getComputed_: d(function (ownerId, keyPath) {
		return this.computedDb.invokeAsync('find', { _id: keyPath + ':' + ownerId })(
			function (cursor) {
				return cursor.nextPromised()(function (record) {
					return cursor.closePromised()(record ? {
						value: record.unserialized ? serializeValue(record.value) : record.value,
						stamp: record.stamp
					} : getNull);
				});
			}
		);
	}),
	_getReduced_: d(function (key) {
		return this.reducedDb.invokeAsync('find', { _id: key })(function (cursor) {
			return cursor.nextPromised()(function (record) {
				return cursor.closePromised()(record ? {
					value: record.unserialized ? serializeValue(record.value) : record.value,
					stamp: record.stamp
				} : getNull);
			});
		});
	}),
	_storeDirect_: d(function (ownerId, path, data) {
		var unserialized = Boolean(data.value && isUnserializable(data.value[0]));
		var record = {
			ownerId: ownerId,
			value: unserialized ? unserializeValue(data.value) : data.value,
			stamp: data.stamp
		};
		if (path) {
			record.path = path;
			record.keyPath = resolveKeyPath(ownerId + '/' + path);
		}
		if (unserialized) record.unserialized = true;
		return this.directDb.invokeAsync('update', { _id: ownerId + (path ? ('/' + path) : '') },
			record, updateOpts);
	}),
	_storeComputed_: d(function (ownerId, keyPath, data) {
		var unserialized = Boolean(data.value && isUnserializable(data.value[0]));
		var record = {
			ownerId: ownerId,
			keyPath: keyPath,
			value: unserialized ? unserializeValue(data.value) : data.value,
			stamp: data.stamp
		};
		if (unserialized) record.unserialized = true;
		return this.computedDb.invokeAsync('update', { _id: keyPath + ':' + ownerId },
			record, updateOpts);
	}),
	_storeReduced_: d(function (ns, path, data) {
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
	}),
	_loadDirect_: d(function (query, filter) {
		return this.directDb.invokeAsync('find', query)(function (cursor) {
			return cursor.toArrayPromised()(function (records) {
				var result = create(null);
				records.forEach(function (record) {
					if (filter && !filter(record.ownerId, record.path)) return; // filtered
					result[record._id] = {
						value: record.unserialized ? serializeValue(record.value) : record.value,
						stamp: record.stamp
					};
				});
				return cursor.closePromised()(result);
			}.bind(this));
		}.bind(this));
	})
}, lazy({
	directDb: d(function () {
		return this.driver.mongoDb.invokeAsync('collection', this.name);
	}),
	computedDb: d(function () {
		return this.driver.mongoDb.invokeAsync('collection', this.name + '-computed');
	}),
	reducedDb: d(function () {
		return this.driver.mongoDb.invokeAsync('collection', this.name + '-reduced');
	})
})));
