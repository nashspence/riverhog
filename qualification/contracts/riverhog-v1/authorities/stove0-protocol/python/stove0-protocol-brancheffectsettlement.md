# stove0_protocol.BranchEffectSettlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-brancheffectsettlement:83daa75c75 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7bd6083665"></a>
| Field | Shape |
|---|---|
| <a id="s-3cc50df764"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4a686b1851"></a>`distribution` | "stove0-protocol" |
| <a id="s-7db621204d"></a>`module` | "stove0_protocol" |
| <a id="s-e0bd5621f6"></a>`name` | "BranchEffectSettlement" |
| <a id="s-66e22b302a"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchEffectSettlement.seal](stove0-protocol-brancheffectsettlement-seal.md)
- [stove0_protocol.BranchEffectSettlement.verify_digest](stove0-protocol-brancheffectsettlement-verify-digest.md)

## Governing policies

- <a id="pa-8b00be9794"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchEffectSettlement`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17b4262852524102f2108210f7b00af5717169136a96e651afd34d2d60da66f4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "8d079076de524eab27cd12f57634171620d2cd76204c4d4349a9231ece5830d8",
    "signature": "\"(*, format: Literal['stove0-branch-effect-settlement/v1'] = 'stove0-branch-effect-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effect_receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchEffectSettlement",
  "unit": "export"
}
```
