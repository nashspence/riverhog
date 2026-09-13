# stove0_operator_contracts.EvaluationUpdatedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationupdatedevent:8564549796 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-062f7eb7ef"></a>
| Field | Shape |
|---|---|
| <a id="s-c0a5b8de01"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-5ccc6043b3"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-19ba929f51"></a>`module` | "stove0_operator_contracts" |
| <a id="s-fd39019df9"></a>`name` | "EvaluationUpdatedEvent" |
| <a id="s-d58988d1b3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3f67e8b575"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationUpdatedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 01fe2cbce1e64705267e483a83fe5348667b690f4772bfdb91b030f75dc18aff -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "b4bc91ca87029c356c47ff9a7274db3f8f871bfa655b6696c04562571522aabc",
    "signature": "\"(*, specversion: Literal['1.0'] = '1.0', id: Annotated[str, MinLen(min_length=1)], source: Literal['urn:riverhog:stove0'], type: Literal['io.riverhog.stove0.evaluation.updated'], subject: Annotated[str, MinLen(min_length=1)], time: str, datacontenttype: Literal['application/json'] = 'application/json', data: stove0_operator_contracts.EvaluationUpdatedEventData) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationUpdatedEvent",
  "unit": "export"
}
```
