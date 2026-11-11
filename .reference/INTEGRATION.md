# Focused integration map

All paths below are audited at `0e45d35a64d18dbde0f88b89d3c55d60b4977d28`.
This is source-controlled reference input; current decisions and source still
control. No discovery runtime activation is part of publication.

## First useful cut

Put the shared owner with neutral reusable packages, not Core or Native. The
reference package has no imports of either implementation. Use the repository's
locked build dependencies, supported platform wheels, packaging and declaration
policy; its standalone setuptools fixture is not a proposed dependency-policy
exception. The existing client Windows native build demonstrates packaging
precedent, not permission to import its unrelated implementation.

Replace the frame-local ownership in `workspace-reclamation` with a mandatory
retained turn. Keep its exact-name/private-file checks, caller ordering, physical
octet budget, truncation/unlink/fsync semantics and declared growth. Neither the
old signature with hidden transient owners nor a raw-close compatibility adapter
should remain on the maintained pre-v1 path.

A representative owner flow is:

```python
# self and its original exclusive parent custody are already retained/admitted.
self.extent = RetiredFileTurn(directory, names=names, octet_budget=grant)
progress = self.extent.advance()  # owners already retained if this raises
# Only a positive receiving result allows the caller to consume success.
```

The outer close path must first receive self.extent, then dependent Core/Native
owners, and only then its parent lease. It keeps that exact ordering when advance
or close raises. The actual execution owner joins the worker separately before
releasing its original execution/resource charge. Do not install a finally block
which discards self.extent after an exception.

## Owning paths and required change

| Current owner | Concrete integration cut |
| --- | --- |
| `packages/workspace-reclamation/src/workspace_reclamation/__init__.py:reclaim_retired_files` | Replace local `descriptor`/`handle` finally-close with a retained directory/data owner. Persistently attach both to the caller's turn before acquisition. |
| `packages/riverhog-provenance/src/riverhog_provenance/journal_reclamation.py` and `corpus_reclamation.py` | Carry the original neutral extent receiver through Native's retained retirement object; Core must not reach into Native to invent its result. |
| `riverhog/src/riverhog_core/canonical_discovery_metadata_reclamation.py:reclaim_discovery_metadata` | Receive the mandatory extent turn through the outer DiscoveryRetirementTurn before calling advance. Directory-phase publication follows synchronized absence and actual child receiving, not merely return from a helper with lost locals. |
| `canonical_discovery_retirement.py:DiscoveryRetirementTurn` | Retain an extent slot before the metadata call, replace `_open` and lease close closures with native owners, and set custody `_held=False` only after the original lease owner receives. |
| `retained_workspace_handles.py:RetainedWorkspaceHandles` | Remove raw-integer callback semantics. Retain exact receiving objects and stop at the first unresolved dependency. No generic callback that returns None can manufacture resource proof. |
| `canonical_discovery_parent_reclamation.py:DiscoveryParentRetirement` | Pre-retain anchor/root/coordinator/directory/lease owners before open. Keep namespace-sync and catalog deletion ordering. A lost close reply never invokes os.close on its old integer. |
| `canonical_discovery_retired_metadata.py:RetiredDiscoveryMetadata` | Convert `_descriptor`, `_child_root`, `_root_coordinator` and child raw handles. Preserve the independent actual SQLite connection receiver; neutral file closure says nothing about SQLite's private handles. |
| `journal_validation_retirement.py` and `canonical_discovery_child_reclamation.py` | Apply the same before-acquisition retention to directory/coordinator/child open paths; pass existing custody, not only its identity. |
| `journal_validation_workspace_reference.py:retain_validation_workspace` | Use a pre-retained duplicate owner from the actual held lease. The useful existing object-identity principle remains, but do not infer success from FileIO.closed after a failed first close. |

Keep the existing Core/Native storage/cursor close protocols. A neutral descriptor
receiver cannot certify a database connection, a Native worker, metadata receiving,
archive custody or namespace disappearance. Their actual owners remain composed
in the outer turn. The narrow implementation must be integrated at every touched
raw file/directory acquisition; an unchanged hidden callback would retain the bug.

## Actual-owner fault witnesses

Start from integration's retained #987 reproduction, not a fresh mock turn.
Inject the selected *backend* before-close refusal for the exact data owner while
metadata disposal has already performed its admitted three-byte mutation. The
same DiscoveryRetirementTurn.close must retain then actually receive that data
owner; fstat is only the test oracle. Parent lease/custody and both budget classes
stay charged until that receiving plus actual worker join.

For lost parent-close reply, record the native result first, raise on delivery,
then place an unrelated test-owned file in precisely that descriptor slot. Retry
the same retained turn. It must replay receiving without any second native close.
Repeat after reopening the very same inode and with a relative directory owner.
Do not substitute different dev/ino as the only negative control.

Also repeat when acquisition returns a descriptor but kind/header validation
fails, when the reply after acquisition is lost, when a borrower has not received
although its thread joined, and when a native close result is unknown. No parent
receiving, fresh retry turn, or catalog completion may bypass that unknown owner.

## Existing evidence/qualification ownership

Integrate source-owned declarations into the maintained `riverhog-growth-contracts`
projection and the existing closure/audit machinery. The included declarations
and samples already use that vocabulary and validator; they do not enroll a new
product component into gates by themselves. Preserve the neutral extent helper's
own name/byte-growth declarations and declared delegates through Core and Native.

Add shared tests to existing unit, native and installed-package selection so
Linux/macOS execute them. No platform skip may be called feature qualification.
Retain exact compiled-wheel/source relationships, original failed reproductions,
actual syscall/refusal counts, and distinct correctness/growth/timing results.
The integration's local unpublished d51db81d bytes must be reconciled by its owner;
this reference was not tested against that local checkpoint.
