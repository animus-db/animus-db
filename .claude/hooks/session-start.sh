#!/bin/bash
# SessionStart hook, two jobs:
#
#   1. Inject the repo's session operating mode (CLAUDE.md "Session operating
#      mode") into the agent's context. stdout of a SessionStart hook is added
#      to the conversation, which makes the posture a live boot-time
#      instruction instead of a mid-file bullet sessions read past — the
#      maintainer was having to re-issue it by hand every session (see
#      docs/engineering-lessons.md, Parallel-agent orchestration).
#   2. Make `gh stack` available to Claude Code on the web. This repo ships
#      larger work as a stacked PR series (see CLAUDE.md's Conventions). The
#      tooling for that is github/gh-stack, a `gh` CLI extension, plus its
#      agent skill — neither of which is present in a fresh web container.
#      Without them an agent hand-rolls the stack, which strands the top PR
#      when the base merges first (issue #279; see
#      docs/engineering-lessons.md).
#
# Idempotent and non-interactive: safe to re-run, installs only what is missing.
set -euo pipefail

# --- 1. Operating-mode injection (every session, local and web) -------------
# Keep this a faithful summary of CLAUDE.md "Session operating mode"; that
# section is the source of truth.
cat <<'EOF'
ANIMUS-DB SESSION OPERATING MODE (maintainer standing instruction — binding;
full text in CLAUDE.md "Session operating mode"):
1. Main thread orchestrates only. Delegate anything token-heavy — code
   exploration, multi-file implementation, gate runs — to Sonnet subagents
   (one investigation agent per issue, one implementation agent per change).
   Inline main-thread work is for trivial tasks only: a one-liner, a doc
   tweak, a targeted read/grep.
2. Run subagents in the BACKGROUND. The main thread stays responsive to the
   maintainer throughout — brief progress notes while agents work, never a
   silent session blocked on a foreground agent.
3. Deliver work as a gh-stack PR series whenever it has more than one
   reviewable logical step; a single flat PR is the exception and its
   description says why.
EOF

# --- 2. Tooling install (web containers only) -------------------------------
# Local checkouts already have whatever the developer installed; only the
# ephemeral web container needs this.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

GH_VERSION="2.63.2"
INSTALL_DIR="${HOME}/.local/bin"
mkdir -p "$INSTALL_DIR"

# 1. The `gh` CLI itself.
if ! command -v gh >/dev/null 2>&1 && [ ! -x "${INSTALL_DIR}/gh" ]; then
  echo "session-start: installing gh ${GH_VERSION}" >&2
  tmp="$(mktemp -d)"
  curl -sSL --retry 3 -o "${tmp}/gh.tgz" \
    "https://github.com/cli/cli/releases/download/v${GH_VERSION}/gh_${GH_VERSION}_linux_amd64.tar.gz"
  tar xzf "${tmp}/gh.tgz" -C "$tmp"
  install -m 0755 "${tmp}/gh_${GH_VERSION}_linux_amd64/bin/gh" "${INSTALL_DIR}/gh"
  rm -rf "$tmp"
fi
export PATH="${INSTALL_DIR}:${PATH}"

# The image ships Go under /usr/local/go/bin, which is not on the default PATH.
# The source-build fallback below needs it.
[ -d /usr/local/go/bin ] && export PATH="${PATH}:/usr/local/go/bin"

# 2 + 3. The gh-stack extension and its agent skill, from one clone.
#
#   The release-download path (`gh extension install github/gh-stack`) needs the
#   GitHub *API* for a repo outside this session's scope, which the web
#   container's proxy refuses with HTTP 403 even when github.com itself is
#   reachable. Anonymous git reads DO work, so we clone and build from source as
#   the fallback. Go is present in the image; the build takes ~2 minutes and is
#   cached with the container afterwards.
export GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
SKILL_DIR="${HOME}/.claude/skills/gh-stack"
NEED_EXT=1
gh stack --help >/dev/null 2>&1 && NEED_EXT=0
NEED_SKILL=1
[ -f "${SKILL_DIR}/SKILL.md" ] && NEED_SKILL=0

if [ "$NEED_EXT" = 1 ] || [ "$NEED_SKILL" = 1 ]; then
  tmp="$(mktemp -d)"
  if git clone --depth 1 -q https://github.com/github/gh-stack "${tmp}/gh-stack" 2>/dev/null; then

    if [ "$NEED_SKILL" = 1 ] && [ -d "${tmp}/gh-stack/skills/gh-stack" ]; then
      echo "session-start: installing the gh-stack skill" >&2
      mkdir -p "$(dirname "$SKILL_DIR")"
      cp -r "${tmp}/gh-stack/skills/gh-stack" "$SKILL_DIR"
    fi

    if [ "$NEED_EXT" = 1 ]; then
      # Preferred: prebuilt release. Falls back to a source build when the
      # proxy refuses the releases API.
      if ! gh extension install github/gh-stack >/dev/null 2>&1; then
        if command -v go >/dev/null 2>&1; then
          echo "session-start: building the gh-stack extension from source" >&2
          # Install the built binary into gh's extension directory directly.
          # `gh extension install <dir>` only SYMLINKS the directory, which
          # dangles the moment this hook removes its temp clone.
          ext_dir="${HOME}/.local/share/gh/extensions/gh-stack"
          ( cd "${tmp}/gh-stack" \
            && go build -o gh-stack . >/dev/null 2>&1 \
            && mkdir -p "$ext_dir" \
            && install -m 0755 gh-stack "${ext_dir}/gh-stack" ) \
            || echo "session-start: gh-stack extension build failed (continuing)" >&2
        else
          echo "session-start: no gh-stack release access and no Go toolchain (continuing)" >&2
        fi
      fi
    fi
  else
    echo "session-start: could not clone github/gh-stack (continuing)" >&2
  fi
  rm -rf "$tmp"
fi

# 4. `gh stack` reads these; set them so an agent never has to rediscover them.
git config rerere.enabled true || true
git config remote.pushDefault origin || true

# Keep gh on PATH for the rest of the session.
if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  echo "export PATH=\"${INSTALL_DIR}:\$PATH\"" >> "$CLAUDE_ENV_FILE"
fi

echo "session-start: gh $(gh --version 2>/dev/null | head -1 | awk '{print $3}') ready; stack extension $(gh stack --help >/dev/null 2>&1 && echo present || echo MISSING)" >&2
