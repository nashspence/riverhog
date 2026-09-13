# stove0_operator_contracts.WorkCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-workcreatedevent:bb663f2571 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b09e77382"></a>
| Field | Shape |
|---|---|
| <a id="s-4468de22d4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-e7ae0e2909"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-6df2c46ea2"></a>`module` | "stove0_operator_contracts" |
| <a id="s-dfe6c0a4e6"></a>`name` | "WorkCreatedEvent" |
| <a id="s-423b9c3071"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8a2b43ce0a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.WorkCreatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9362f3baf451fe81dfcf73763304ce5fa7761e43b9d3808d62628cf2e158a6c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "14ba98a6cc43790e66a7c554f5956f029ada8c5216b7d5d71455bbf86d3500bf",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.work.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.WorkCreatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "WorkCreatedEvent",
  "unit": "export"
}
```
