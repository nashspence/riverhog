# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #952. Convention: #903. Related predecessor: #950.
Audited base: `main` @ `802ed38225b4ecd9aacd1b7dc3de51a1b1fd1905`.
Audited base tree: `079c49f5ea400d891d2ee5e6453a38afebfb51b9`.

Produced by: OpenAI ChatGPT.
Model: GPT-6 Astra Pro (user-visible identity).
Configuration/effort: not exposed; no hidden configuration claim.
Requested by: Nash Spence, Riverhog maintainer.
Published by: OpenAI ChatGPT through the connected GitHub tool, on the maintainer's behalf.
Date: 2026-10-03.

## Purpose

Settle meaning versus editorial presentation, define a multi-file release corpus,
source-derived requirements and explicit review fence, and show testable compiler
and native projection seams. Start at `952/DESIGN.md`; `952/RESEARCH.md` records primary
sources and implementation consequences. `source.schema.json` is the strict runnable
subset. `compiler.py` and `test_compiler.py` contain synthetic witnesses, not a real
v1 documentation corpus or a completed production integration.

The experiment compiles captured Markdown and summaries against SOURCE-owned
requirements, rejects unowned/duplicate targets and invented waivers, checks local
links, binds exact source bytes, distinguishes preview from final completeness, and
keeps explicit review state separate from release authorization. Small adapters show
literal argparse help, operation-level OpenAPI annotations, owned Python docstrings,
and prepared project metadata without changing the underlying business semantics.

## Validation actually performed

- Acquired a full Git clone pinned to the audited SHA through the read-only clone
  gateway, not a reconstructed source-only snapshot. Archive SHA-256:
  `34618aa86bd5cf8abb63429213232ae84bdc4237141d6fc969d15cf8ccb0f29c`.
- Verified the gateway manifest SHA-256 and all 1,359 exported file hashes, sizes and
  modes (including Git payload files), then verified actual HEAD/tree, clean status,
  and `git fsck --full` before adding this reference.
- Ran the isolated compiler/schema/Git/native-seam test suite locally on Python
  3.13.5, markdown-it-py 4.2.0 and jsonschema 4.26.0. Results for the final supplied
  tree and exact pushed reference SHA are recorded in the owning issue.
- Compiled Python reference modules. No production source or workflow was changed
  except a new branch-specific read-only reference-check workflow.
- Inspected actual Closure examples from an earlier retained artifact with the
  recorded `1c17175e...` identity. This was not fresh native generation.

## Limits and integration notice

This is ADDITIVE REFERENCE MATERIAL, not a production patch. The full native
requirement/discovery producer, pointer ownership resolver, source field disposition,
semantic rebaseline, full Click/schema/Python/image adapters, package-local resources,
sdist/wheel rebuilding, workflow migration and end-to-end release qualification
remain integration work. The compiler trusts its requirement ledger as a source-owned
input; a caller cannot claim completeness merely by supplying `complete: true`.

The runnable subset supports JSON/Markdown, not assets or shards. Its intermediate
HTML/link routes are not a second production site. Its plain-text projection and
native seams are witnesses, not full output-compatibility implementations. The schema
uses the proposed v2 source vocabulary but is not a published public format.

Reference report hashing is deliberately named separately; replace it with native
Riverhog canonicalization and existing production manifests during integration.
Installing the missing native `rfc8785==0.1.4` dependency failed on DNS resolution.
The full pinned Python 3.12.3 / uv 0.11.24 Riverhog environment, native generation,
all repository gates, browser/OCI/platform/provider/release qualification were NOT
run. Focused reference checks must not be called full Riverhog qualification.

No main/release code branch, documentation-authoring tree, product/maintenance tag,
release, Pages deployment, or history rewrite is changed by this reference. The
GitHub commit's publication metadata does not replace the producer record above.

Creation, testing, publication, native issue linkage, or request by an authorized
repository agent does not establish or extend accepted design, contract, release
requirements, or integration authority. The owning issue and subsequent maintainer
decisions control. Reconcile through the then-current normal integration rail; do
not merge mechanically. The exact reference SHA in #952 is the handoff identity;
the branch name is navigation only. Do not force-push away an active published handoff.

Available connector actions do not expose native Development linked-branch creation.
Record that limitation in #952 instead of claiming a Markdown URL is native linkage
or creating an auto-closing PR as a substitute.
