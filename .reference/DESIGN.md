# One verified traversal, with explicit ownership

## Decision and scope

The [request](https://github.com/nashspence/riverhog/issues/991#issuecomment-7233007754) identifies repeated history work after recovery metadata preparation. The reviewed current source is `60612eded1e0e4e5188c28a84697435eb969fad0`; the neutral owners and recovery caller are unchanged from the requested `8d278b6f7bc443a9919bcc5a6409a647cfd20f43`. This proposal uses their existing authority, worklists, complete-set verifier and receiving machinery. It changes no caller's authorization or selected archive identity.

The smallest useful reduction is to visit a history's descriptor and complete sets once, stage the resulting obligations in the closure's existing SQLite state, then let that existing worklist admit inherited histories once per exact history/extent. A new general cache or decryptor pool would add ownership and invalidation work before removing the demonstrated repetition.

## Exact fact and lifetime boundaries

| Operation/fact | What is freshly checked | Permitted reuse and limit |
| --- | --- | --- |
| `MemberHistoryStore.read_descriptor(binding)` | Exact bytes, digest, extent, canonical descriptor and member identity | Returns descriptor facts only; neither set nor journal completeness is implied. |
| `read_source_proof(imported)` | Complete source membership proof and all imported identity fields | No set or journal authority. Every actual import edge invokes it. |
| Existing `descriptor()` / `source_proof()` | Their complete existing descriptor/set verification | Independent calls remain fresh; no implicit Store cache. |
| Closure history work | Complete roots/imports, commitment/order/terminal checks, exact bound primary, unique bound heads, imported State and required journals | Existing deduplication is keyed by exact history and extent, within one owned closure. A complete binding mismatch rejects even if its digest was already registered. |
| Imported primary State | Exact historical primary delivery and State comparison | Default resolver performs the existing fresh journal-prefix read on every edge. A caller-supplied resolver retains its separate current-source and prechecked-state lifetime contract. |
| `summary_at()` and documentary references | Existing exact prefix/hash/current-read-fence checks | Existing bounded validation snapshots may be reused; fresh source reads remain. Borrowed readers retain their original separate leases. |
| Structural export | Fresh object read, exact admitted size/hash, then structural path identity | Each exported object must match the bytes admitted in this operation. A path alone is insufficient for pages. |
| New attempt / changed selection / closed owner | Original admission and current-source paths | No new cross-attempt cache or durable admission. After source reading, export checks that its closure is still open before exposing bytes. |

Direct repeated `resolve()` already reuses completed closure obligations. The proposal does not promise a new physical read of every set on each repeated import. It reuses a verified immutable set fact while continuing fresh edge proof/descriptor/State checks. A new closure rejects changed or incomplete sets. Fresh export rejects changed pages even when their deterministic path is unchanged. Existing detached membership images and borrowed journal snapshots retain their own bounded, current-source and receiving contracts; this patch does not expand their authority or lifetime.

## Streaming without accepting a prefix

`descriptor(..., stage_page=...)` invokes a tentative callback after each bounded page's records have been checked. At that instant, the aggregate commitment may still fail. The callback therefore supplies no independently accepted result.

The closure surrounds the **entire public resolution**, including all inherited work and journal draining, with one savepoint in its existing work transaction. It stages roots, imports, object fingerprints and pending history obligations using existing tables. A late malformed terminal, source read-fence failure, interrupted callback, invalid inherited set or journal failure rolls back every new membership/worklist row. Previously completed selections remain intact. Independently validated immutable journal snapshots may remain in the existing cache; subsequent reuse still reads the live source as before.

The work transaction is explicitly begun only when none is active. This matters: releasing an outermost SQLite savepoint would otherwise commit every member. The candidate retains the existing transaction instead; `membership_image()` keeps its original explicit commit, and a subsequent selection starts a new work transaction. There is no new durability setting, periodic checkpoint or commit cadence.

Unconfirmed cursor close or failed rollback freezes the owner. The same retained cursor/connection/directory must be explicitly received. The helper closes the actual object iterator on success and on type, extent or source failure; an uncertain source close prevents successful admission/export. Cancellation is not treated as proof of receiving.

## Why exact page fingerprints are necessary

A record-page path commits its **set authority and ordinal**, not the page's full bytes. The finite adverse experiment removed a non-primary root while retaining that path. Original export accepted the changed page; an intervening original import happened to catch it through another complete set verification. The initial simplification removed that redundant traversal and exposed the missing export check.

The final proposal stores a SHA-256 digest and exact byte count beside each path in the **existing** object inventory. These are derived from the verified canonical descriptor/page/proof representation. Strict canonical codecs preserve the original bytes, as the maintained witness checks. A repeated object with a different fingerprint rejects. Export rereads the provider, receives the original stream, checks the owner is still open, compares size/hash, and retains the existing structural identity check before yielding that object.

This is small identity metadata, not retained plaintext or a reusable payload cache. It also prevents publishing a different page partition during the same admitted operation even if a future complete traversal could validate that partition. A new attempt can establish a new valid physical representation through the normal full verifier.

## Actual consumers

**Independent recovery:** `_stage_mapping` owns one closure over all members in an attempt. Payload-index identity must match each authenticated binding; retained extent includes complete selected/inherited history. The separate post-resolution `closure.store.descriptor()` remains, as do fresh structural export, staged/canonically validated journal bytes and final description/tag head-pin comparisons before the complete receipt. The patch does not silently classify that caller's extra pass as dispensable.

**Core terminal validation:** `_validate_staged_canonical_journal_set` owns one synchronous attempt with source/job/transaction fences and prechecked raw historical data. Interruption restarts that barrier. Neutral imported proof/State/extent checks remain; no staged result is promoted by reference publication.

**Core canonical reads/discovery and client transfer:** existing prepared readers and protected disk replay have their own operation or selection lifetime. Authorization/root fences, later-selection fresh reads, current claim checks, bounded membership images and source-root checks after transfer remain. The new neutral code imports no application implementation and makes no global cache shared among their closures.

## Dependencies and parallel work

Bindings select descriptors; descriptors select complete structural sets; membership proofs select inherited bindings; primary journals establish exact State; all required history must drain before success. These ordering requirements remain. Removing duplicated decryptions reduces work without increasing simultaneous resource ownership.

The recovery process's wait in official `age` does not show spare capacity. Repository-pinned age 1.3.1 uses scrypt with `N=2^logN`, `r=8`, `p=1`, and pins `golang.org/x/crypto v0.45.0`. That implementation allocates a V array of `32*N*r` uint32 elements. **At logN18 this one array is 256 MiB per active derivation**, inferred from source, before other arrays/runtime/buffers/plaintext scratch. This is not measured RSS or admission. See the exact upstream links in [SOURCES.json](SOURCES.json).

The actual recovery wrapper owns the original synchronous age process, password pipe and temporary plaintext destination. Official plugin source owns plugin pipe closure and `Wait`. The experiment observes every outer age invocation returning zero; it does not independently instrument plugin PIDs or cancellation races. A future bounded parallel proposal would need measured useful independent work, child CPU/RSS/overlap, admitted aggregate KDF memory and original process/pipe/file receiving. This handoff proposes no pool, wider worker limit or weaker KDF.

## Remaining costs and integration ownership

Identical sets shared by distinct history descriptors still get one full pass per history/extent. Default imported-primary journal reads remain per edge. Recursive cycle queries still traverse descendants; this patch proves no complete closure-growth bound. The separate recovery descriptor pass and fresh export remain. These are explicit accounting results, not permission to enlarge numerical budgets or restart large campaigns.

The completed-preparation request (`7234184351`) is a different slice. Recovery #1009 and activation #1010 remain completed integrations. #1006 continues policy/budget adoption; #1007 retains downstream whole-path/growth qualification. Current, comparable GitHub Actions evidence and independently approved complete-operation maxima remain required when claiming that contract. This local reference supplies neither numerical approval nor a performance verdict.
