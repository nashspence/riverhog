# stove0_protocol.BranchTargetPreview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchtargetpreview:2dfcfad20d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-38f8266ad1"></a>
| Field | Shape |
|---|---|
| <a id="s-8ca1d1daec"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-9cd4996fff"></a>`distribution` | "stove0-protocol" |
| <a id="s-8e3d51fad0"></a>`module` | "stove0_protocol" |
| <a id="s-d7c7f6915b"></a>`name` | "BranchTargetPreview" |
| <a id="s-d2d02d4b70"></a>`unit` | "export" |

## Governing policies

- <a id="pa-736af10e9f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchTargetPreview`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b51081cf4338e6e4209dcb227378c8ffaf653013d522dc2241068e369bfef1e4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "c4d10f0d0aedba13309965e3328e4ebf83e86cab353244860f70f7d01e3d2647",
    "signature": "\"(*, branch_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], workflow_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_plan: stove0_protocol.models.TargetPlanBinding) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "BranchTargetPreview",
  "unit": "export"
}
```
