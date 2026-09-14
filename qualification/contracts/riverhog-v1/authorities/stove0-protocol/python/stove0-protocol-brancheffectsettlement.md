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
- <a id="s-4a686b1851"></a>`distribution`: `stove0-protocol`
- <a id="s-7db621204d"></a>`module`: `stove0_protocol`
- <a id="s-e0bd5621f6"></a>`name`: `BranchEffectSettlement`
- <a id="s-66e22b302a"></a>`unit`: `export`

### Declared structure

- <a id="s-a06512b8a3"></a>`kind`: `"class"`
- <a id="s-b3e7998028"></a>`signature`: `"\"(*, format: Literal['stove0-branch-effect-settlement/v1'] = 'stove0-branch-effect-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effect_receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-9fb71ba400"></a>
- <a id="s-aa7643379f"></a>`title`: BranchEffectSettlement
- <a id="s-915a62d91e"></a>`description`: Success-only receipt identity for one required external-effect branch.
- <a id="s-ecb292acb2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2b40572333"></a>`branch_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e048dc94a3"></a>`effect_receipt_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f408df7e04"></a>`format` | no | type="string"; const="stove0-branch-effect-settlement/v1" |  |
| <a id="s-476e450987"></a>`settlement_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b7677d118d"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-74ffdfff9b"></a>`workflow_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

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

<!-- exact-contract-value: d7df64fbf8bdbba172f6fc78cbc78ba0f77fc072a0fa0d7f6c063e60496af075 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "description": "Success-only receipt identity for one required external-effect branch.",
      "properties": {
        "branch_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Branch Id",
          "type": "string"
        },
        "effect_receipt_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Effect Receipt Sha256",
          "type": "string"
        },
        "format": {
          "const": "stove0-branch-effect-settlement/v1",
          "default": "stove0-branch-effect-settlement/v1",
          "title": "Format",
          "type": "string"
        },
        "settlement_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Settlement Sha256",
          "type": "string"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        },
        "workflow_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Workflow Plan Sha256",
          "type": "string"
        }
      },
      "required": [
        "branch_id",
        "work_id",
        "workflow_plan_sha256",
        "effect_receipt_sha256",
        "settlement_sha256"
      ],
      "title": "BranchEffectSettlement",
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-branch-effect-settlement/v1'] = 'stove0-branch-effect-settlement/v1', branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], effect_receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchEffectSettlement",
  "unit": "export"
}
```
