# stove0_operator_contracts.EvaluationUpdatedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationupdatedeventdata:1880c9f4b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8621a40e56"></a>
- <a id="s-64150470d7"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f11b35e119"></a>`module`: `stove0_operator_contracts`
- <a id="s-ea15c17811"></a>`name`: `EvaluationUpdatedEventData`
- <a id="s-5f20d84bf6"></a>`unit`: `export`

### Declared structure

- <a id="s-e699378557"></a>`kind`: `"class"`
- <a id="s-d06bc7b0ee"></a>`signature`: `"\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None\""`

#### Validated model schema

<a id="s-77969a9546"></a>
- <a id="s-5ab54d703b"></a>`title`: EvaluationUpdatedEventData
- <a id="s-98bb941528"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a0c4d5b694"></a>`evaluation_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9dfc5dfc90"></a>`phase` | yes | type="string"; enum=["planning","running","partially_complete","complete","failed","canceled"] |  |
| <a id="s-c11da2d2fd"></a>`revision` | yes | type="integer"; minimum=2 |  |

## Governing policies

- <a id="pa-0a407365fb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationUpdatedEventData`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64b4bf505ed4284fe0af528eecdaef243337bd4db7199c149544e5c55b041b2d -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "evaluation_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Evaluation Id",
          "type": "string"
        },
        "phase": {
          "enum": [
            "planning",
            "running",
            "partially_complete",
            "complete",
            "failed",
            "canceled"
          ],
          "title": "Phase",
          "type": "string"
        },
        "revision": {
          "minimum": 2,
          "title": "Revision",
          "type": "integer"
        }
      },
      "required": [
        "evaluation_id",
        "phase",
        "revision"
      ],
      "title": "EvaluationUpdatedEventData",
      "type": "object"
    },
    "signature": "\"(*, evaluation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['planning', 'running', 'partially_complete', 'complete', 'failed', 'canceled'], revision: Annotated[int, Ge(ge=2)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationUpdatedEventData",
  "unit": "export"
}
```
