'use strict';

var setPrototypeOf = require('es5-ext/object/set-prototype-of')
  , ensureObject   = require('es5-ext/object/valid-object')
  , ensureString   = require('es5-ext/object/validate-stringifiable-value')
  , d              = require('d')
  , deferred       = require('deferred')
  , Driver         = require('dbjs-persistence/driver')
  , Mongo          = require('mongodb')
  , Storage        = require('./storage')

  , isIdent = RegExp.prototype.test.bind(/^[a-z][a-z0-9A-Z]*$/)

  , promisify = deferred.promisify
  , MongoCursor = Mongo.Cursor, MongoClient = Mongo.MongoClient
  , connect = promisify(MongoClient.connect);

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
	return url + '/' + conf.databaseName;
};
var MongoDriver = module.exports = Object.defineProperties(function (data) {
	if (!(this instanceof MongoDriver)) return new MongoDriver(data);
	ensureObject(data);
	ensureObject(data.mongo);
	ensureString(data.mongo.database);
	Driver.call(this, data);
	this.mongoDb = connect(buildUrl(data.mongo)).aside(null, this.emitError);
}, { storageClass: d(Storage) });
setPrototypeOf(MongoDriver, Driver);

MongoDriver.prototype = Object.create(Driver.prototype, {
	constructor: d(MongoDriver),

	__resolveAllStorages: d(function () {
		return this.mongoDb.invokeAsync('collections')(function (collections) {
			collections.forEach(function (collection) {
				var name = collection.collectionName;
				if (!isIdent(name)) return;
				this.getStorage(name);
			}, this);
		}.bind(this))(Function.prototype);
	}),
	__close: d(function () { return this.mongoDb.invokeAsync('close'); })
});
