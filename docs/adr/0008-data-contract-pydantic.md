# 0008. Data contract: Pydantic at the ingestion boundary

- **Status:** Accepted
- **Date:** 2026-07-05

## Context

Kafka is schema-agnostic — a topic is just bytes, and malformed or drifted
messages will flow downstream and silently corrupt aggregates if nothing checks
them (we literally have two hand-produced junk messages sitting in the topic
from Day 3 testing). We need a formal, enforced definition of a valid trade at
the point data enters the system, expressing types, nullability, and invariants
a bare JSON blob cannot.

## Decision

We will define the ingestion **data contract** as a **Pydantic** model
(`consumer/schema.py`) with `Literal` enums (`type`, `side`), `Decimal` for
price/size, `> 0` and non-empty invariants, and `extra="forbid"` so unknown
fields surface as violations. A verification consumer validates the live topic
against it; unit tests lock the invariants. Details in
[`docs/data_contract.md`](../data_contract.md).

## Consequences

- Bad data fails fast at the boundary, cheaply, with clear errors — proven live
  (all real trades valid; the two junk messages flagged).
- Money is exact (`Decimal`, parsed straight from JSON, never via `float`) — a
  finance non-negotiable.
- `extra="forbid"` turns upstream drift into a loud signal, at the cost of having
  to consciously update the contract for benign additive changes (accepted).
- Contract is Python-only and single-repo — fine because we own both ends here.

## Alternatives considered

- **Schema registry + Avro/Protobuf (Confluent Schema Registry)** — the
  org-scale answer: language-neutral, compact binary, enforced backward/forward
  compatibility rules. Overkill for a single-repo project; documented as the
  clear next step when producers/consumers span teams/languages.
- **`@dataclass` / manual `if` checks** — no built-in validation/coercion;
  reinvents Pydantic poorly. Rejected.
- **No contract (trust the source)** — the failure mode this ADR exists to
  prevent. Rejected.
