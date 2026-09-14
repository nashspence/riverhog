# stove0_operator_contracts.EvaluationReviewIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationreviewin:7a73328a72 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b249a0d91"></a>
- <a id="s-94de2d6c30"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-d5f979f1e9"></a>`module`: `stove0_operator_contracts`
- <a id="s-ee33da7404"></a>`name`: `EvaluationReviewIn`
- <a id="s-af4b6314cf"></a>`unit`: `export`

### Declared structure

- <a id="s-ee74c3ad2e"></a>`kind`: `"class"`
- <a id="s-749736db76"></a>`signature`: `"\"(*, rating: Annotated[int \| None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None) -> None\""`

#### Validated model schema

<a id="s-c3c9c96bae"></a>
- <a id="s-180af637df"></a>`title`: EvaluationReviewIn
- <a id="s-d3100af1b2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f6ed07d62"></a>`note` | no | anyOf=type="string"; minLength=1; maxLength=4000; pattern="^\\S(?:[\\s\\S]*\\S)?$" \| type="null" |  |
| <a id="s-a7f49cdf49"></a>`rating` | no | anyOf=type="integer"; minimum=1; maximum=5 \| type="null" |  |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationReviewIn.meaningful](stove0-operator-contracts-evaluationreviewin-meaningful.md)

## Governing policies

- <a id="pa-e62ff7e6b4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationReviewIn`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ceff4a715a8b619a949fa7b0b097574b4c4b5141605720c3f38fa1994751da97 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "anyOf": [
        {
          "properties": {
            "rating": {
              "type": "integer"
            }
          },
          "required": [
            "rating"
          ]
        },
        {
          "properties": {
            "note": {
              "type": "string"
            }
          },
          "required": [
            "note"
          ]
        }
      ],
      "properties": {
        "note": {
          "anyOf": [
            {
              "maxLength": 4000,
              "minLength": 1,
              "pattern": "^\\S(?:[\\s\\S]*\\S)?$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Note"
        },
        "rating": {
          "anyOf": [
            {
              "maximum": 5,
              "minimum": 1,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Rating"
        }
      },
      "title": "EvaluationReviewIn",
      "type": "object"
    },
    "signature": "\"(*, rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationReviewIn",
  "unit": "export"
}
```
