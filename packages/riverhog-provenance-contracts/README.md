# riverhog-provenance-contracts

Closed JSON Schema resources, canonical encoding, exact contract bindings, and
identifier/reference rules for Riverhog's canonical provenance journals.

[`SPECIFICATION.md`](SPECIFICATION.md) defines the contract. Application validation
also checks graph, journal, and cross-record invariants that JSON Schema alone cannot
express; `riverhog-provenance` provides that runtime validation.
