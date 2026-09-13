# stove0_protocol.BranchSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsettlement:29cad94916 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-caa0797d38"></a>
| Field | Shape |
|---|---|
| <a id="s-d94eb75497"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4ce63a798f"></a>`distribution` | "stove0-protocol" |
| <a id="s-49a5e90287"></a>`module` | "stove0_protocol" |
| <a id="s-c3ced75411"></a>`name` | "BranchSettlement" |
| <a id="s-36f8a832ac"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchSettlement.seal](stove0-protocol-branchsettlement-seal.md)
- [stove0_protocol.BranchSettlement.verify_digest](stove0-protocol-branchsettlement-verify-digest.md)

## Governing policies

- <a id="pa-92b2d5278a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSettlement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 743601c710af816d90e2aa536f9f5274e8b83501658e7fc1aed65102d012e273 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "bfc41425873e5fbc835023b004e10a0a7759c5726572d11f26def01105144ab1",
    "signature": "\"(*, format: Literal['stove0-branch-settlement/v1'] = 'stove0-branch-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], producer_settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_protocol.models.CollectionRootRef, output_selection: stove0_protocol.fork_join.ArtifactSelectionRef, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchSettlement",
  "unit": "export"
}
```
