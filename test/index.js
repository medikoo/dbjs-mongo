'use strict';

var getTests = require('dbjs-persistence/test/_common')

  , env, copyEnv, tests;

try {
	env = require('../env');
} catch (e) {
	env = null;
}

env.collection = 'dbjs-mongo-test-' + Date.now();
copyEnv = Object.create(env);
copyEnv.collection = 'dbjs-mongo-test-copy-' + Date.now();

tests = getTests(env, copyEnv);

module.exports = function (t, a, d) {
	return tests.apply(null, arguments).done(function () { d(); }, d);
};
