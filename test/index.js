'use strict';

var getTests          = require('dbjs-persistence/test/_common')
  , storageSplitTests = require('dbjs-persistence/test/_storage-split')

  , env, copyEnv, splitEnv, tests;

var genPostfix = function () {
	return (new Date()).toISOString().replace(/\./g, '-').replace(/:/g, '-');
};

try {
	env = require('../env');
} catch (e) {
	env = null;
}

if (env) {
	env.database = 'dbjs-mongo-test-' + genPostfix();
	copyEnv = Object.create(env);
	copyEnv.database = 'dbjs-mongo-test-copy-' + genPostfix();

	splitEnv = Object.create(env);
	splitEnv.database = 'dbjs-mongo-split-' + genPostfix();

	tests = getTests({ mongo: env }, { mongo: copyEnv });
}

module.exports = function (t, a, d) {
	if (!env) {
		console.error("No database configuration (env.json), unable to proceed with test");
		d();
		return;
	}
	return tests.apply(null, arguments)(function () {
		return storageSplitTests(t, { mongo: splitEnv }, a);
	}).done(function () { d(); }, d);
};
