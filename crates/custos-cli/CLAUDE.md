# CLAUDE.md — custos-cli

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The operator CLI (`custos`). **Placeholder `main` only.**

## When you build this

- Operator commands: cluster status, tablet-map inspection, membership and
  split/merge changes — i.e. a client of the control plane's `MetaCommand`s and
  the data-plane routing view.
- Keep it a thin client over the library crates.
