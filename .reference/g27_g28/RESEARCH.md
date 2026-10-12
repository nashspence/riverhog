# Source audit and research notes

Audited repository source is pinned to
[`152d095038df0c0d0ed642cf265d47b11c65a754`](https://github.com/nashspence/riverhog/commit/152d095038df0c0d0ed642cf265d47b11c65a754).
The external branch parent is `6c66476fc88738b51526c2295f95b5720bb60140`.
Issue decisions, not this research, remain authoritative. Relevant policy:
[#964 operation-scoped decision](https://github.com/nashspence/riverhog/issues/964#issuecomment-6111028968),
[G26 derived identity approval](https://github.com/nashspence/riverhog/issues/964#issuecomment-6112668478),
[retained Alembic clarification](https://github.com/nashspence/riverhog/issues/964#issuecomment-6114619051),
[current onboarding/growth ownership clarification](https://github.com/nashspence/riverhog/issues/964#issuecomment-6114967253),
[#963 measurement foundation](https://github.com/nashspence/riverhog/issues/963),
[#903 reference convention](https://github.com/nashspence/riverhog/issues/903).

## Findings from inspected source

1. [`browse.py:keyset_statement`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/browse.py#L19)
   already implements a correct lexicographic OR boundary with explicit null
   ordering for the examined tuples. It is not an OFFSET implementation. However,
   the native OR/NULL predicate can force prefix re-scanning even on an ordered
   compound index. The probe uses that actual SQLAlchemy helper with a two-column
   non-null/tied-key table. The selected path uses a tuple boundary and removes
   redundant null-order modifiers under the explicit non-null proof.
   Both paths must enumerate identical rows. A small synthetic table is deliberate:
   it isolates this physical-plan problem without claiming a full catalogue run.

2. [`canonical_discovery_search.py:_read_identity`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/canonical_discovery_search.py#L125)
   hashes the header and every selected collection on each invocation. The native
   discovery path invokes it on every page. `_exact_support` (line 293) reparses
   matching raw assertions for each candidate member/clause. `_matching_members`
   uses expanded membership joins and DISTINCT. G26 removes the expanded storage
   representation but does not by itself remove repeated query work or repeated
   whole-set cursor validation. G27/G28 must specify both preparation and paging.

3. [`assertion_clause_matches`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/canonical_discovery_extraction.py#L181)
   iterates every posting to filter EACH matching profile section before testing
   its predicates. With G declared sections and V postings, that loop performs
   G*V startswith checks even for zero results. The proposed trie routes one posting
   pass, preserving all four byte/text representations, type tests, section order
   and first support selection. Existing code sorts object keys during scalar
   traversal; that source cost is not removed or relabeled constant by this change.

4. [`publish_index_build`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/canonical_discovery_index.py#L574)
   updates active generation state without calling
   [`publish_catalog_event`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/catalog_events.py#L90).
   Reusing only the catalog sync committed watermark for cache validity is unsafe.
   The recommended separate private fence covers all query-visible base/header
   mutations. Its unchanged test is deliberately conservative. A changed epoch
   with equal native read digest leads to rebuilding, not a new public conflict.

5. [`_collection_summary_query`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/services/collections.py#L421)
   uses multiple correlated SUM/COUNT and manifest MIN/MAX subqueries. Small output
   is not proof of a cheap point query. The design supplies precise old/new delta
   aggregates and a fixed-number-of-probes read, retaining the existing state
   conditions and missing/ambiguous manifest failure. No summary implementation
   or mutation-parity qualification is supplied in this reference.

6. [`_search_candidates`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/services/search.py#L201)
   unions member matches from member ID/digest, collection text/description and
   tags. Explicit outer scope is not distributed into every branch in source.
   Selecting matching collections first and semijoining members avoids avoidable
   collection/tag fan-out. General `%literal%` search is still a scan/candidate
   profile, not a claimed indexed prefix or arbitrary-language evaluator.

7. [`list_plan_artifacts`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/services/retrieval.py#L744)
   already uses sealed ordinal paging. The shared
   [`read_snapshot`](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/packages/state-schema/src/state_schema/__init__.py#L386)
   is PostgreSQL repeatable-read/read-only and explicitly closes sessions. Mutable
   browse, exact discovery and sealed authority pages must not be merged into one
   consistency claim. Internal generator snapshot lifetime remains a distinct
   owned resource cost from HTTP page lifetime.

## Controlled observations (reproducible in counters.json)

| Member-style text-ID SQL fixture | Native OR helper VM instructions | Non-null tuple VM instructions |
|---|---:|---:|
| 128 rows, page 25 | 5,799 | 1,396 |
| 512 rows, page 25 | 49,574 | 5,857 |
| 1,024 rows, page 25 | 169,182 | 11,807 |
| 2,048 rows, page 25 | 632,659 | 23,750 |

These rows use a 64-character text ID and short leading-key tie groups. With
ALL leading keys tied at 2,048 members, native OR uses 2,526,988 instructions and
the selected tuple/plain-order path uses 29,693. A separate integer-rowid-alias
negative control still partially seeks: its tuple path uses 5,013 / 54,315 /
198,801 instructions for 128 / 512 / 1,024 tied rows. A SEARCH label therefore
is not proof of a full-key seek. The selected equality-prefix partition fallback
has passing work/ordering controls at 512/2,048 rows in both directions; see
`seek_partitions` and `test_rowid_prefix_partition_seek_work`.

These are actual SQLite progress-handler counts with each SQL statement fully
consumed, identical output verified, not elapsed time. Raw first/deep-page SQL
and plans are retained in a deduplicated plan catalogue. Native continuation
plans show a SCAN of the covering index; member-text tuple continuation uses a
full composite SEARCH range without a temporary ordering B-tree. No PostgreSQL plan is inferred
from SQLite evidence, and no whole-product speedup is asserted.

At 512 profile sections the controlled assertion has 2,049 scalar postings. The
actual native matcher performs 1,049,088 section `startswith` calls. The candidate
visits 2,049 postings and performs 512 predicate calls, plus explicitly recorded
trie-character work. The input here exercises the native lexical helper, not a
validated complete journal/archive wire fixture. Positive/type/byte/pointer,
first-declared-section and same-section-negative controls are in the test file.

For 2,048 controlled selected-collection headers and 41 invocations of the actual
native read-identity helper, the counter is 83,968 header visits. The separate
projection lifecycle model scans its 2,048 headers at preparation and performs
41 page validations without another manifest scan in the unchanged-view control.
The model does NOT implement the product canonical hash or actual authorization;
production must retain native identity and current authorization checks. The
counter is not proof that the proposed production triggers cover every mutation.

The proposal follows from source structure and these component observations;
asymptotic formulas are reasoned proposed bounds, not established from four
samples. Tests prevent semantic shortcuts; production owner instrumentation and
both-backend witnesses are still required.

## Primary external references consulted

- [SQLite row values / scrolling window queries](https://www.sqlite.org/rowvalue.html):
  composite boundary comparisons support efficient scrolling with the matching
  key index; OFFSET work grows with skipped rows. This supports the selected seek
  form, not a claim that the existing code used OFFSET.
- [PostgreSQL indexes and ORDER BY](https://www.postgresql.org/docs/current/indexes-ordering.html):
  B-tree order, null placement and multicolumn direction must match the request;
  LIMIT alone does not eliminate a sort's input work.
- [PostgreSQL combining indexes](https://www.postgresql.org/docs/current/indexes-bitmap-scans.html):
  bitmap combinations lose index ordering and can require a separate sort. A
  nominally indexed OR filter is not automatically an ordered page seek.
- [SQLite expression and LIKE semantics](https://www.sqlite.org/lang_expr.html#like):
  default ASCII case behavior means SQL candidates cannot replace native exact
  verification in canonical discovery. Preserve each public surface's own semantics.
- [PostgreSQL pg_trgm](https://www.postgresql.org/docs/current/pgtrgm.html):
  substring acceleration is data/pattern dependent; patterns without extractable
  trigrams can scan the index. Keep the explicit scan fallback profile.
- [PostgreSQL transaction isolation](https://www.postgresql.org/docs/16/transaction-iso.html)
  and [SET TRANSACTION](https://www.postgresql.org/docs/current/sql-set-transaction.html):
  coherent multi-statement reads require the appropriate snapshot; a sequence
  value is not a rolled-back commit counter. Do not retain transactions across
  client think time or substitute a sequence for the proposed transactional fence.
- [RFC8785 upstream v0.1.4 implementation](https://github.com/trailofbits/rfc8785.py/blob/v0.1.4/src/rfc8785/_impl.py):
  exact externally supplied dependency used for the native test environment;
  it is not copied into the reference and no canonicalization contract is changed.

Current documentation was checked during this research. Integration must use the
repository's actually maintained backend versions, not silently substitute the
versions on a `/current/` documentation page. No external source is used to claim
that Riverhog-specific scope, authority or identity semantics may change.
