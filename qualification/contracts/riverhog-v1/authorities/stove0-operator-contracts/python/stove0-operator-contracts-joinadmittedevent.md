# stove0_operator_contracts.JoinAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-joinadmittedevent:c2d272407b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2fd208df44"></a>
| Field | Shape |
|---|---|
| <a id="s-68488d9d61"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-17313ffa83"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-bf41737332"></a>`module` | "stove0_operator_contracts" |
| <a id="s-42e8593db4"></a>`name` | "JoinAdmittedEvent" |
| <a id="s-e33552aba3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-729dab7ac8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JoinAdmittedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23a4d05aac4ab11cf6f4f2700818508419b68d8ce1763760fe8d97c1a38cb788 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "0b6a8760c5eeffee01b5c936e89477f622b1f7e244905710451296fd4f0c7422",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.join.admitted'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.JoinAdmittedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "JoinAdmittedEvent",
  "unit": "export"
}
```
