'use strict';

var getTests = require('dbjs-persistence/test/_common')

  , env, copyEnv, tests;

try {
	env = require('../env');
} catch (e) {
	env = null;
}

if (env) {
	env.collection = 'dbjs-mongo-test-' + (new Date()).toISOString();
	copyEnv = Object.create(env);
	copyEnv.collection = 'dbjs-mongo-test-copy-' + (new Date()).toISOString();

	tests = getTests({ mongo: env }, { mongo: copyEnv });
}

module.exports = function (t, a, d) {
	if (!env) {
		console.error("No database configuration (env.json), unable to proceed with test");
		d();
		return;
	}
	return tests.apply(null, arguments).done(function () { d(); }, d);
};
