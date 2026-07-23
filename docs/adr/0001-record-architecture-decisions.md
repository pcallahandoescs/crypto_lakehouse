# 0001. Record architecture decisions

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

This project makes many non-obvious technical choices (Kafka vs. alternatives,
Lambda vs. Kappa, Delta vs. Iceberg, partitioning strategy, ...). The *reasoning*
behind each is the most valuable and most perishable knowledge in the system —
it's what a reviewer or a future maintainer needs, and it's exactly what gets
lost when it lives only in someone's head or a chat log.

## Decision

We will keep an **Architecture Decision Record** for each significant decision,
as short numbered markdown files under `docs/adr/`, using a Nygard-style template
(Context / Decision / Consequences / Alternatives). ADRs are immutable: a
superseded decision is replaced by a new ADR, not edited away.

## Consequences

- The "why" behind the system is durable and reviewable.
- Small ongoing discipline: each real decision costs a few minutes to record.
- Each ADR is a self-contained record of one tradeoff and its alternatives.

## Alternatives considered

- **Document only in the README** — decisions get buried and edited over time,
  losing the historical record of *why*.
- **No formal record** — fastest now, but the reasoning evaporates. Rejected.
