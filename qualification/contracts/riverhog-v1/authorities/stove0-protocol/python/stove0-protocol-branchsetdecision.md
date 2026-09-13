# stove0_protocol.BranchSetDecision

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetdecision:fecb4aab4c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-000575956d"></a>
| Field | Shape |
|---|---|
| <a id="s-ac958cf3f9"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-08ef765399"></a>`distribution` | "stove0-protocol" |
| <a id="s-6bf012a543"></a>`module` | "stove0_protocol" |
| <a id="s-7b883408c3"></a>`name` | "BranchSetDecision" |
| <a id="s-95e760f5fd"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchSetDecision.branch_set_documents](stove0-protocol-branchsetdecision-branch-set-documents.md)
- [stove0_protocol.BranchSetDecision.canonical_branch_sets](stove0-protocol-branchsetdecision-canonical-branch-sets.md)
- [stove0_protocol.BranchSetDecision.canonical_selections](stove0-protocol-branchsetdecision-canonical-selections.md)
- [stove0_protocol.BranchSetDecision.complete_documents](stove0-protocol-branchsetdecision-complete-documents.md)
- [stove0_protocol.BranchSetDecision.leaf_branches](stove0-protocol-branchsetdecision-leaf-branches.md)
- [stove0_protocol.BranchSetDecision.selection_documents](stove0-protocol-branchsetdecision-selection-documents.md)

## Governing policies

- <a id="pa-74e8123523"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetDecision`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7f412b9e58f3a0a18e6fc56305cbef6ac4dc3461ba81e0de2875c3d65bf227e1 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7002a62c586004f4b0d38994ee4ab49a26cfdf6186d266401bb6e891beca18aa",
    "signature": "'(*, plan: stove0_protocol.fork_join.BranchSetPlan, selections: tuple[stove0_protocol.fork_join.ArtifactSelection, ...], branch_sets: tuple[stove0_protocol.fork_join.BranchSetPlan, ...] = ()) -> None'"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSetDecision",
  "unit": "export"
}
```
