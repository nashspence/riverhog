# stove0_operator_contracts.Stove0CloudEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0cloudevent:e0f8ea2258 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-efd4c54b15"></a>
| Field | Shape |
|---|---|
| <a id="s-723b24641f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-7648196d49"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-118e11fbe2"></a>`module` | "stove0_operator_contracts" |
| <a id="s-5fdf247562"></a>`name` | "Stove0CloudEvent" |
| <a id="s-51a090cd7d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.Stove0CloudEvent.exact_subject](stove0-operator-contracts-stove0cloudevent-exact-subject.md)

## Governing policies

- <a id="pa-c54ab721eb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0CloudEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3a1a907fdf1ff99dcb66a9980d68051199504c3c1e7feed7b61c4dd7cbd7c97 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "0a714ffc7a24bde3a4c45a3624ec4bec304011f02c9cb8d6b0ec2cbeeb945700",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: Any) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0CloudEvent",
  "unit": "export"
}
```
