# stove0_operator_contracts.SchedulerStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-schedulerstatus:0cc34ce399 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d85b8fcc4"></a>
| Field | Shape |
|---|---|
| <a id="s-ff2f79badb"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-744391c4fd"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-ab919852d7"></a>`module` | "stove0_operator_contracts" |
| <a id="s-da05acebf9"></a>`name` | "SchedulerStatus" |
| <a id="s-506bb2f405"></a>`unit` | "export" |

## Governing policies

- <a id="pa-23dd0659f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.SchedulerStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06f28a88a0571775564c7d533048a07a1e6bf04b4c138d3fa589ebebeaea35bf -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7a4eac0cdfe53ec109a9dd097dd163c7a16e9c52f9f04684ef04035f8d303bd5",
    "signature": "\"(*, running: bool, interval_seconds: Annotated[float, Gt(gt=0)], roles: tuple[typing.Literal['controller', 'worker', 'combined'], ...]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "SchedulerStatus",
  "unit": "export"
}
```
