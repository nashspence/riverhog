# Source audit and primary references for the bounded G22/G26 revision

Audited main: 152d095038df0c0d0ed642cf265d47b11c65a754.
Reference parent: 4621be70fe2229ef7de169862ff1ec5274dec00f.
The owning issue and normal integration rail control adoption. Recommendations
in this directory supersede incompatible earlier reference lifecycle advice.

## Source findings and chosen corrections

The native journal.py validate_journal_chunks function returns one summary after
prefix, profile, predecessor, source and exact-tail checks. It keeps a hashlib
prefix state and hashes frame.encoded; checkpoint comparisons are native flat
SHA-256, not a digest that can be substituted by a chunk tree. Hence G22 retains
its exact digest and requires a reviewed resumable continuation backend.

JournalStore in journal_store.py already implements close and freeze, along with
weakref cleanup. Its disposable DB uses synchronous=OFF. That is not a power-loss-
safe durable validation checkpoint. Resource ownership must use existing close
support deliberately, not claim close is missing or finalizers are sufficient.
The historical G22 EpochFrames loops over whole epochs and high-degree adjacency
and deletes/reset tables without bounded phase cursors. Its native parity tests
remain useful, not proof of the new physical execution model.

canonical_discovery_rows.py constructs IndexedAssertion with complete posting,
profile and edge tuples. canonical_discovery_index.py lines 55-59 and 224 onward
apply per-assertion/page/posting text limits. The exact source guard rejects 20,001
postings. The generic JSON definition accepts 20,001 small scalar values; this is
not proof of a complete externally admitted assertion reaching that call. The
corrected design requires a fully valid native profile/journal witness during
integration, and removes derived-only totals rather than raising their constants.

complete_index_build (line 499 onward) counts entire relations, loops through all
snapshots and calls _dataset_sha256 before publishing complete. The G26 replacement
uses independent source obligations and a separately versioned compact Merkle
pipeline; final publication checks fixed terminal metadata rather than performing
that whole traversal under one Session. The new digest is an explicit identity
refinement, not evidence that historical policy approved this exact encoding.

publish_index_build (line 573 onward) switches active_build_id under collection/
root/epoch conditions. G27/G28's retained searchable directory requires atomic
publication of its corresponding source descriptor too; catalog-event watermark
alone is not that coordination. G26 supplies generations, not a second query cache.

The original source-normal intervals, exact history bindings and same-member
import AND guards remain. Both requested history extents and State byte extent
are independent. All source-fact sharing remains subordinate to archive authority,
current access and exact native source validation. Current-schema generation
retention is not old-pre-v1 schema support. Alembic infrastructure stays intact.

## Primary technical sources checked

- [SQLite WAL](https://www.sqlite.org/wal.html): a held reader can obstruct complete
  checkpoint/reset. Bounded transactions and real between-step checkpoint controls
  address that resource issue; they do not alone prove fair production scheduling.
- [SQLite limits](https://www.sqlite.org/limits.html): strings/BLOBs and rows have
  physical limits. Segmented byte carriers are needed; batching rows is insufficient.
- [PostgreSQL vacuum](https://www.postgresql.org/docs/current/routine-vacuuming.html):
  reclamation and old transactions are maintenance concerns. PostgreSQL was not run.
- [Python hashlib](https://docs.python.org/3/library/hashlib.html): update/copy/digest
  describe incremental hashing, not a portable durable export of provider state.
- [RFC 6234](https://www.rfc-editor.org/rfc/rfc6234.html): SHA-256 compression,
  chaining words, residual-block handling and final padding. The independent
  diagnostic is compared against native hashlib; it is not production certification.
- [RFC 9162 section 2.1](https://www.rfc-editor.org/rfc/rfc9162.html#section-2.1):
  domain-separated ordered binary Merkle trees and deterministic split/fold. G26
  uses that algorithmic shape with its OWN source-normal leaf/header domain.
- [ITP 2019 paper](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ITP.2019.18)
  and [author explanation](https://gallium.inria.fr/blog/incremental-cycle-detection/):
  correctness/amortized complexity of the modified incremental cycle algorithm.
  The proof does not automatically cover our SQLite adapter or restart/undo protocol.

## Alternatives and trade-offs

Keeping one transaction per epoch/correction is simpler but does not bound physical
work. Naively restarting all validation or sealing after each worker replacement
multiplies completed work. Storing accepted-prefix checkpoints alone is insufficient
when one correction/edge has a large unfinished graph operation; save its subphase
and undo progress too. Durable state adds writes, integrity and cleanup obligations.

For G26, a serializable flat SHA state could retain the former compact transcript,
but needs another owner-reviewed backend/context codec. The selected ordered Merkle
commitment uses standard native hash calls on bounded records, with resumable carry
and fold and a deliberately different derived identity. G22 cannot use that option
without changing archived prefix commitments and therefore retains exact SHA-256.
Neither approach permits trusting a digest without independently complete source
and semantic validation. Work budgets never enter deterministic content identity.
