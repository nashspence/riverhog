# stove0_operator_contracts.EvaluationCreatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationcreatedevent:234180fdf8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9e93a2755"></a>
| Field | Shape |
|---|---|
| <a id="s-d7168f1304"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-8212001da0"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-6ce32f60bb"></a>`module` | "stove0_operator_contracts" |
| <a id="s-a72cde56eb"></a>`name` | "EvaluationCreatedEvent" |
| <a id="s-6311f4ff52"></a>`unit` | "export" |

## Governing policies

- <a id="pa-f745cb5c96"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationCreatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ae31d0aa3c32199e7490f84f428627d3136be7de27cbf202261b0d2356ed720 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "52570919e6a0bf09290e62d59812406cfb485c0add7710a05ffac8be63119baa",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.created'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationCreatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationCreatedEvent",
  "unit": "export"
}
```
