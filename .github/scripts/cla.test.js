// Unit tests for the pure half of cla.js. Run: node --test .github/scripts/
'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');
const cla = require('./cla.js');

const config = cla.loadConfig(path.join(__dirname, '..', '..'));
const author = { login: 'alice', id: 1 };
const commit = (sha, login, id, email, parents = 1) => ({
  sha, parents, authorLogin: login, authorId: id, authorEmail: email,
});

test('the checked-in config is complete', () => {
  for (const key of ['version', 'document', 'phrase', 'signaturesBranch', 'signaturesFile', 'exempt', 'assistantEmails']) {
    assert.ok(config[key] !== undefined, `cla.json lacks ${key}`);
  }
});

test('CLA.md states the signing sentence and version from cla.json', () => {
  const doc = require('fs').readFileSync(path.join(__dirname, '..', '..', config.document), 'utf8');
  assert.ok(doc.includes(config.phrase), 'CLA.md must quote the exact signing sentence');
  assert.ok(doc.includes(`**Version ${config.version}**`), 'CLA.md version must match cla.json');
});

test('signing sentence: whitespace, case and a missing full stop do not matter', () => {
  assert.ok(cla.isSigningComment(config.phrase, config));
  assert.ok(cla.isSigningComment(`  ${config.phrase.toUpperCase().replace(/\.$/, '')}\n`, config));
  assert.ok(cla.isSigningComment(config.phrase.replace(/ /g, '  \n'), config));
});

test('signing sentence: anything else does not sign', () => {
  assert.ok(!cla.isSigningComment(`I do not agree. ${config.phrase}`, config));
  assert.ok(!cla.isSigningComment('', config));
  assert.ok(!cla.isSigningComment('I have read the AnimusDB CLA', config));
});

test('the PR author must sign even with no commits of their own', () => {
  const { required, unlinked } = cla.requiredSigners({ author }, [], config);
  assert.deepEqual(required, [author]);
  assert.deepEqual(unlinked, []);
});

test('every linked commit author must sign, once each, PR author first', () => {
  const { required } = cla.requiredSigners({ author }, [
    commit('a', 'bob', 2, 'bob@x'),
    commit('b', 'alice', 1, 'alice@x'),
    commit('c', 'bob', 2, 'bob@x'),
  ], config);
  assert.deepEqual(required.map((r) => r.login), ['alice', 'bob']);
});

test('exempt maintainers and bots never need to sign', () => {
  const { required } = cla.requiredSigners({ author: { login: 'GuillaumeFont', id: 9 } }, [
    commit('a', 'dependabot[bot]', 3, 'd@x'),
    commit('b', config.exempt[0], 9, 'g@x'),
  ], config);
  assert.deepEqual(required, []);
});

test('assistant commits are covered by the PR author, not their GitHub login', () => {
  const { required, unlinked } = cla.requiredSigners({ author }, [
    commit('a', 'claude', 81847, 'NoReply@Anthropic.com'),
  ], config);
  assert.deepEqual(required, [author]);
  assert.deepEqual(unlinked, []);
});

test('an unlinked commit author blocks the PR', () => {
  const { unlinked } = cla.requiredSigners({ author }, [commit('abc', null, null, 'ghost@x')], config);
  assert.deepEqual(unlinked, [{ sha: 'abc', email: 'ghost@x' }]);
});

test('merge commits are skipped', () => {
  const { required, unlinked } = cla.requiredSigners({ author }, [
    commit('m', null, null, 'ghost@x', 2),
    commit('n', 'carol', 4, 'c@x', 2),
  ], config);
  assert.deepEqual(required, [author]);
  assert.deepEqual(unlinked, []);
});

test('signatures match by account id, so a renamed account stays signed', () => {
  const missing = cla.missingSigners(
    [{ login: 'alice-renamed', id: 1 }, { login: 'bob', id: 2 }],
    [{ login: 'alice', id: 1 }],
  );
  assert.deepEqual(missing, [{ login: 'bob', id: 2 }]);
});

test('status comment: asks for the sentence, names who is missing, carries the marker', () => {
  const body = cla.statusBody([{ login: 'bob', id: 2 }], [], config, 'https://github.com/o/r');
  assert.ok(body.startsWith(cla.MARKER));
  assert.ok(body.includes('@bob'));
  assert.ok(body.includes(config.phrase));
  const done = cla.statusBody([], [], config, 'https://github.com/o/r');
  assert.ok(done.startsWith(cla.MARKER));
  assert.ok(!done.includes(config.phrase));
});
