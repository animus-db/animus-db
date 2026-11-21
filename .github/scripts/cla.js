// Contributor License Agreement (CLA.md) bookkeeping, shared by two callers:
//
//   - .github/workflows/cla.yml (pull_request_target + issue_comment): records
//     a signature when someone posts the signing sentence, keeps one status
//     comment on the PR up to date, and re-runs the PR's failed CI jobs once
//     everyone has signed. It needs write access, so it runs from the base
//     branch and never checks out PR code.
//   - the `cla` job in .github/workflows/ci.yml (pull_request, read-only
//     token): fails the PR until everyone who must sign has signed. That job
//     is what `gates` aggregates, so an unsigned PR cannot enter the queue.
//
// Who must sign: the PR author, and the GitHub account of every non-merge
// commit's author, except the Licensor's own accounts (`exempt`), bot
// accounts (`[bot]`) and coding assistants (`assistantEmails`, whose commits
// the PR author's signature covers). A commit whose author e-mail is linked to
// no GitHub account cannot be matched to a signature, and blocks the PR.
//
// Signatures live as one JSON array in `signaturesFile` on the orphan branch
// `signaturesBranch`; the workflow creates the branch on the first signature.
// They are matched by numeric GitHub account id, so a renamed account keeps
// its signature.
//
// The pure functions are unit-tested by cla.test.js (`node --test`), which
// the `cla` CI job runs first.

'use strict';

const fs = require('fs');
const path = require('path');

const MARKER = '<!-- animus-cla-status -->';

function loadConfig(root) {
  const file = path.join(root, '.github', 'cla.json');
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

// Case, surrounding whitespace, inner runs of whitespace and a trailing full
// stop do not matter; any other text in the comment does.
function normalizePhrase(text) {
  return String(text || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/\.$/, '')
    .toLowerCase();
}

function isSigningComment(body, config) {
  return normalizePhrase(body) === normalizePhrase(config.phrase);
}

function isBot(login) {
  return /\[bot\]$/i.test(login || '');
}

function isExempt(login, config) {
  const l = (login || '').toLowerCase();
  return isBot(login) || config.exempt.some((e) => e.toLowerCase() === l);
}

// pr: { author: { login, id } }
// commits: [{ sha, parents, authorLogin, authorId, authorEmail }]
// Returns { required: [{ login, id }], unlinked: [{ sha, email }] }, both
// deduplicated and in first-seen order (PR author first).
function requiredSigners(pr, commits, config) {
  const assistants = new Set(config.assistantEmails.map((e) => e.toLowerCase()));
  const required = [];
  const seen = new Set();
  const add = (login, id) => {
    if (isExempt(login, config) || seen.has(id)) return;
    seen.add(id);
    required.push({ login, id });
  };
  add(pr.author.login, pr.author.id);

  const unlinked = [];
  for (const c of commits) {
    if ((c.parents || 1) > 1) continue; // merge commit: no content of its own
    if (assistants.has((c.authorEmail || '').toLowerCase())) continue;
    if (c.authorId == null) {
      unlinked.push({ sha: c.sha, email: c.authorEmail || '' });
      continue;
    }
    add(c.authorLogin, c.authorId);
  }
  return { required, unlinked };
}

function missingSigners(required, signatures) {
  const signed = new Set(signatures.map((s) => s.id));
  return required.filter((r) => !signed.has(r.id));
}

// The status comment's text. `missing` and `unlinked` come from the functions
// above; an empty pair means everyone has signed.
function statusBody(missing, unlinked, config, repoUrl) {
  const doc = `${repoUrl}/blob/main/${config.document}`;
  if (missing.length === 0 && unlinked.length === 0) {
    return `${MARKER}\nEveryone on this pull request has signed the [AnimusDB CLA](${doc}). Thank you. (If the \`cla\` check is still red from an earlier run, a maintainer re-runs it.)`;
  }
  const lines = [MARKER, '**Contributor License Agreement needed.**', ''];
  lines.push(
    `AnimusDB is licensed under the AGPL-3.0 and is also offered under a commercial licence, so contributions need the [AnimusDB Contributor License Agreement](${doc}) (version ${config.version}). You keep the copyright in your work; the agreement lets the project distribute it under both licences, and commits the project to keeping it available under the AGPL.`,
  );
  if (missing.length > 0) {
    lines.push('', `Still to sign: ${missing.map((m) => '@' + m.login).join(', ')}.`);
    lines.push('', 'To sign, read the agreement, then post this exact comment on this pull request:');
    lines.push('', '```', config.phrase, '```');
    lines.push('', 'You sign once; it covers every later pull request from the same GitHub account.');
  }
  if (unlinked.length > 0) {
    lines.push('', 'These commits are authored by an e-mail address that is linked to no GitHub account, so they cannot be matched to a signature. Add the address to the author\'s GitHub account (Settings, Emails), or re-author the commits:');
    for (const u of unlinked) lines.push(`- ${u.sha.slice(0, 10)} (${u.email || 'no e-mail'})`);
  }
  return lines.join('\n');
}

// ---------------------------------------------------------------- GitHub I/O

async function fetchPr(github, owner, repo, number) {
  const { data: pr } = await github.rest.pulls.get({ owner, repo, pull_number: number });
  const raw = await github.paginate(github.rest.pulls.listCommits, {
    owner, repo, pull_number: number, per_page: 100,
  });
  const commits = raw.map((c) => ({
    sha: c.sha,
    parents: (c.parents || []).length,
    authorLogin: c.author ? c.author.login : null,
    authorId: c.author ? c.author.id : null,
    authorEmail: c.commit && c.commit.author ? c.commit.author.email : '',
  }));
  return { pr: { author: { login: pr.user.login, id: pr.user.id }, headSha: pr.head.sha }, commits };
}

// Returns { signatures, sha } where sha is the file's blob sha (null when the
// file or the branch does not exist yet).
async function readSignatures(github, owner, repo, config) {
  try {
    const { data } = await github.rest.repos.getContent({
      owner, repo, path: config.signaturesFile, ref: config.signaturesBranch,
    });
    const text = Buffer.from(data.content, 'base64').toString('utf8');
    return { signatures: JSON.parse(text), sha: data.sha };
  } catch (e) {
    if (e.status === 404) return { signatures: [], sha: null };
    throw e;
  }
}

async function branchExists(github, owner, repo, branch) {
  try {
    await github.rest.git.getRef({ owner, repo, ref: `heads/${branch}` });
    return true;
  } catch (e) {
    if (e.status === 404) return false;
    throw e;
  }
}

const README = `# AnimusDB CLA signatures

Written by \`.github/workflows/cla.yml\`; do not edit by hand. Each entry in
\`signatures/cla.json\` records one GitHub account's acceptance of
\`CLA.md\` on the \`main\` branch, at the version given.
`;

// Appends `entry` unless that account id already signed. Creates the orphan
// branch on first use. Retries on a concurrent write (409/422 on a stale sha).
async function addSignature(github, owner, repo, config, entry) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const { signatures, sha } = await readSignatures(github, owner, repo, config);
    if (signatures.some((s) => s.id === entry.id)) return false;
    const next = JSON.stringify(signatures.concat([entry]), null, 2) + '\n';
    const message = `Record CLA ${config.version} signature of @${entry.login}`;
    try {
      if (!(await branchExists(github, owner, repo, config.signaturesBranch))) {
        const { data: tree } = await github.rest.git.createTree({
          owner, repo,
          tree: [
            { path: 'README.md', mode: '100644', type: 'blob', content: README },
            { path: config.signaturesFile, mode: '100644', type: 'blob', content: next },
          ],
        });
        const { data: commit } = await github.rest.git.createCommit({
          owner, repo, message, tree: tree.sha, parents: [],
        });
        await github.rest.git.createRef({
          owner, repo, ref: `refs/heads/${config.signaturesBranch}`, sha: commit.sha,
        });
      } else {
        await github.rest.repos.createOrUpdateFileContents({
          owner, repo, path: config.signaturesFile, branch: config.signaturesBranch,
          message, content: Buffer.from(next).toString('base64'), ...(sha ? { sha } : {}),
        });
      }
      return true;
    } catch (e) {
      if (e.status !== 409 && e.status !== 422) throw e;
      await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
    }
  }
  throw new Error('could not record the CLA signature after 5 attempts');
}

async function evaluate(github, owner, repo, number, config) {
  const { pr, commits } = await fetchPr(github, owner, repo, number);
  const { required, unlinked } = requiredSigners(pr, commits, config);
  const { signatures } = await readSignatures(github, owner, repo, config);
  return { pr, missing: missingSigners(required, signatures), unlinked };
}

// Keeps a single status comment. Posts one only when something is missing;
// afterwards it is edited in place, including to say everyone has signed.
async function upsertStatusComment(github, owner, repo, number, body, needed) {
  const comments = await github.paginate(github.rest.issues.listComments, {
    owner, repo, issue_number: number, per_page: 100,
  });
  const mine = comments.find((c) => c.user && isBot(c.user.login) && (c.body || '').startsWith(MARKER));
  if (mine) {
    if (mine.body !== body) {
      await github.rest.issues.updateComment({ owner, repo, comment_id: mine.id, body });
    }
  } else if (needed) {
    await github.rest.issues.createComment({ owner, repo, issue_number: number, body });
  }
}

// After a signature, re-run the failed jobs of the PR head's latest CI run so
// the `cla` job (and `gates`) go green without a new push.
async function rerunFailedCi(github, owner, repo, headSha, core) {
  const { data } = await github.rest.actions.listWorkflowRuns({
    owner, repo, workflow_id: 'ci.yml', head_sha: headSha, event: 'pull_request', per_page: 1,
  });
  const run = data.workflow_runs[0];
  if (!run) return;
  if (run.status !== 'completed') {
    core.info(`CI run ${run.id} is still ${run.status}; its cla job reads the signature when it runs.`);
    return;
  }
  if (run.conclusion === 'failure') {
    await github.rest.actions.reRunWorkflowFailedJobs({ owner, repo, run_id: run.id });
    core.info(`Re-ran the failed jobs of CI run ${run.id}.`);
  }
}

module.exports = {
  MARKER,
  loadConfig,
  normalizePhrase,
  isSigningComment,
  requiredSigners,
  missingSigners,
  statusBody,
  readSignatures,
  addSignature,
  evaluate,
  upsertStatusComment,
  rerunFailedCi,
};
