# stove0_operator_contracts.WorkUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workupdatedevent:b70963456b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f34ed81392"></a>
| Field | Shape |
|---|---|
| <a id="s-82fb93c356"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-3d8a289b38"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-3e154e63b5"></a>`module` | "stove0_operator_contracts" |
| <a id="s-80b374214b"></a>`name` | "WorkUpdatedEvent" |
| <a id="s-ea297efab6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-ab31ef4dcf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkUpdatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f5426cad44b091c0988f207a6190b5420801140933825ae863d55724d0b51d2d -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "7acf51ea02c3f6916c1897ebba522acabdab1949ada2cf0aa9dd4ecc4865e666",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.updated'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkUpdatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkUpdatedEvent",
  "unit": "export"
}
```
