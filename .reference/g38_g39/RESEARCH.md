# Research basis and alternatives

Exact inspected source: `main@152d095038df0c0d0ed642cf265d47b11c65a754`.
The original reference branch runtime source predates this SHA; use a separate
checkout. This report separates source findings, controlled executable diagnostics,
proposed correction and production proof still required.

## Source-confirmed findings

1. `compiled_observation_delivery.CompiledObservationDelivery.request` adds a
   bounded LIMIT only for `independent-subjects`; it materializes the whole-scope
   selected query into `tuple(WorkArtifactSubject...)`, then forms an all-ID IN
   selector. This preserves current whole-scope semantics but is not a bounded
   carrier. The source-query probe executes this actual AST and SQLAlchemy path.
2. `observation_questions.physical_question` copies literal options and injects
   complete subject-ID/evidence-slot arrays. The typed request then embeds every
   subject; its invocation embeds selected accepted evidence. Segmentation of a
   preceding transport does not bound these constructors.
3. `accepted_inputs.AcceptedEvidenceInput.pages` is a tuple of ALL pages. Its
   validator traverses and hashes them, and `_validate_observation_evidence_scope`
   traverses them again. Individual bounded pages do not bound the enclosing value.
4. `accepted_observations.accept` canonicalizes/reparses evidence, runs schema and
   semantic acceptance, materializes projected views, and writes all subject
   coverage and projected rows in one transaction. `seal_step` bounds result
   reference iteration but repeats coverage count/anti-join checks. Those owners
   require phased acceptance, not only SQL bulk-insert tuning.
5. `observation_views.project_interface` retains all subject records and status
   maps, then resolves exact endpoint maps and relation records. An unresolved
   associated endpoint in a relevant unusable claim traverses ALL covered subjects
   to downgrade only `complete` statuses. U such claims over N subjects execute
   U*N status lookups even after the first downgrade. One shared taint marker plus
   the final coverage pass preserves the native downgrade rule in linear logical
   visits. Do not discard the original relation records or other statuses.
6. The same routine deduplicates relation edges but preserves source record order
   and multiplicity. `_endpoint_map` rejects conflicting member-instance mappings.
   A new uniqueness constraint on associated ID or dictionary stringification of
   typed endpoint keys would not be an innocent performance optimization.
7. `PersistentObserverService` is already queued/pollable under #948. It loads and
   writes full accepted/status models, retains full invocations and embeds the
   complete result in terminal status. G38/G39 must build on this lifecycle rather
   than report that observers are still solely synchronous or add a second queue.
8. Generic metadata transfer already has exact SHA/length refs and 64 KiB decoded
   chunks. `CanonicalMetadataServer._read_exact` uses `read_bytes`, verification
   reads the full document, and the execution handoff parses a complete model.
   The 4 MiB observer HTTP limit is an INLINE cap, not proof that all larger
   requests fail (metadata staging exists). The remaining issue is end-to-end
   work/materialization, not absence of any bounded network protocol.
9. `ArtifactSelection` canonical order is source-root/member order, not request
   subject-ID order. Both must be preserved with an exact indexed permutation,
   rather than reusing a cursor/hash in the wrong ordering domain.
10. Core provenance and filename-sidecars are actual whole-scope interfaces;
    materialization-hint is independent-subjects. Filename-sidecars consumes
    accepted scoped core evidence, builds all locators and candidates, and cannot
    lawfully turn physical chunks into unrelated complete questions.

## Controlled experiments actually executed

`source_probe.native_delivery` extracts only the native request method. Real
SQLAlchemy and SQLite construct and run its selected/coverage queries. Fixtures
replace typed deserialization with a counted decoder and stop at the physical
request constructor. At 128/512/2048 whole-scope members it decodes all N subjects;
with preferred independent batch 100 it decodes 100. This is direct query-path
work evidence, NOT native request validation, HTTP or completed observation.

`source_probe.native_unresolved_taint` executes the native relation-loop AST on
controlled predicate/pointer/lookup fixtures. At 128/512/2048 subjects and equally
many unresolved relevant claims it executes 16,384 / 262,144 / 4,194,304 status
lookups. The fixture canonicalizer is not the native JCS boundary; all lookup
values are simple controlled strings. This establishes the loop multiplication,
not full semantic/profile equivalence or contribution to a real media wall time.

The separate SQLite model exercises complete exact coverage, preserved status
meaning, a shared taint marker, private acceptance, CAS takeover and bounded
cleanup. At 128/512/2048 subjects, coverage visits exactly N subjects in quanta of
at most 17, terminal status is 42 bytes, and each method returns with zero open
connections. N unresolved claims store N source rows and one marker; they do not
perform N*N subject updates. These are small fixed-row model witnesses, not
production transaction-machine bounds or arbitrary JSON-schema qualification.

The transfer model reuses the prior G22/G26 diagnostic SHA continuation and checks
its digest against native `hashlib` across fragment boundaries and reopening.
It is not a new production crypto backend. The current Stove0 private checkpoint
hash is separately identified as integration input; cross-package imports of core
implementation are expressly not recommended.

## Primary references and their limited role

* [HTTP Semantics RFC 9110, 15.3.3](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.3.3):
  asynchronous acceptance is not completed processing. The observer/controller
  meanings remain Riverhog-owned, not inferred from HTTP status alone.
* [JCS RFC 8785, 3.2.3](https://www.rfc-editor.org/rfc/rfc8785.html#section-3.2.3):
  preserve canonical property ordering and exact encoding. Generic Python sorted
  JSON is not a replacement identity encoder, especially for Unicode keys.
* [JSON Schema 2020-12 validation](https://json-schema.org/draft/2020-12/json-schema-validation):
  whole-instance constraints such as uniqueness/counts do not disappear when
  records are transported incrementally. Their implementation needs explicit
  state and cost; the specification does not promise constant-space validation.
* [SQLite WAL documentation](https://www.sqlite.org/wal.html): transaction release
  matters to checkpoint progress. A disk spool alone does not prove resource
  release. The diagnostic checks connection release/WAL behavior, not PostgreSQL.
* [RFC 9162, 2.1](https://www.rfc-editor.org/rfc/rfc9162.html#section-2.1): considered
  as a bounded-hash-tree alternative. Not selected for G38/G39 transport because
  exact existing metadata/selection SHA domains can be retained with reviewed
  continuation. No new Merkle format or transparency-log conformance is claimed.

The protocol and architecture decisions here are recommendations inferred from
the inspected owner code and issue goals, not externally mandated standards.

## Important limits

No new full native observer schema/conformance suite ran. `rfc8785==0.1.4` was
unavailable from this environment's package index; that install attempt failed.
We did not stub a canonicalizer and call the resulting run native qualification.
Tests use stdlib/SQLite plus the explicitly instrumented AST query/loop probes.

Not run: whole-scope provenance -> sidecar valid-wire pipeline; real semantic
callback/read-view adaptation; arbitrary deep/large JSON canonical parser;
PostgreSQL, HTTP/TLS/capability races, true process kill/power loss/cross-store
recovery, provider execution and original evidence archival recovery; complete
locked format/type/lint/unit/dist/build/local rail or exact-SHA CI/CodeQL.
No timing target, absence-of-regression or complete v1 conformance claim is made.
