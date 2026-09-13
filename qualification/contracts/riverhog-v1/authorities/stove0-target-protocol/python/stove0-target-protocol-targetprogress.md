# stove0_target_protocol.TargetProgress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetprogress:77eee1bcfa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f1971f977"></a>
| Field | Shape |
|---|---|
| <a id="s-d9ab75697e"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-4adc55d8d2"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-ac8b8cc705"></a>`module` | "stove0_target_protocol" |
| <a id="s-b0d75ea25c"></a>`name` | "TargetProgress" |
| <a id="s-6b23ec8d9f"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_target_protocol.TargetProgress.validate_total](stove0-target-protocol-targetprogress-validate-total.md)

## Governing policies

- <a id="pa-93ba9bb76a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProgress`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f3010ea7b06e7c837e75d701574a69f1baf298d61aa9b1298a66e76a367a2b4 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "3b531b19161c4e085385a0c27312bc32072b1dda15095512891696fd64be3fca",
    "signature": "'(*, phase: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], completed: Annotated[int, Ge(ge=0)], total: Annotated[int | None, Ge(ge=0)] = None, unit: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None) -> None'"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetProgress",
  "unit": "export"
}
```
