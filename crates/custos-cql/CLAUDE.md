# CLAUDE.md — custos-cql

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

CQL (Cassandra) wire-protocol adapter over the common core (ADR 0006).
**Skeleton only.**

## When you build this

- The data-model mapping is already demonstrated by `custos-dynamo`: a CQL
  partition key + clustering columns map to a partition key + sort key over the
  `StorageEngine`. Reuse that shape.
- What's CQL-specific: the binary wire protocol framing and a CQL parser/type
  system. That, plus wiring the surface to the distributed request path
  (`custos-data`) rather than a local engine, is the real work.
