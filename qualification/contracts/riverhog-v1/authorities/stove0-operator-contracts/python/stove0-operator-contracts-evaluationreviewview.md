# stove0_operator_contracts.EvaluationReviewView

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-evaluationreviewview:457deed3dc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b2a530699"></a>
- <a id="s-25f42785a7"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f8c8c722c8"></a>`module`: `stove0_operator_contracts`
- <a id="s-36e8466b46"></a>`name`: `EvaluationReviewView`
- <a id="s-30dde84072"></a>`unit`: `export`

### Declared structure

- <a id="s-3bc3bbdc0c"></a>`kind`: `"class"`
- <a id="s-aabd91dfc4"></a>`signature`: `"\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int \| None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""`

#### Validated model schema

<a id="s-877f26a17d"></a>
- <a id="s-8e628a747d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6d9d0031c"></a>`note` | no | anyOf=type="string"; minLength=1; maxLength=4000; pattern="^\\S(?:[\\s\\S]*\\S)?$" \| type="null" |  |
| <a id="s-ed84e9cb19"></a>`rating` | no | anyOf=type="integer"; minimum=1; maximum=5 \| type="null" |  |
| <a id="s-c41a0a10fd"></a>`updated_at` | yes | type="string"; minLength=1; maxLength=40 |  |
| <a id="s-6d39e7707e"></a>`updated_by` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-337d3906a1"></a>`variant_id` | yes | type="string"; minLength=1; maxLength=160 |  |

## Maintained corroboration

### Related interface records

- [stove0_operator_contracts.EvaluationReviewView.meaningful](stove0-operator-contracts-evaluationreviewview-meaningful.md)

## Governing policies

- <a id="pa-d3d4bba199"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.EvaluationReviewView`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bdbfffc20db204bf014ae3242734bf57d49115178b016a153d9c5f9497c263c7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
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
          "default": null
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
          "default": null
        },
        "updated_at": {
          "maxLength": 40,
          "minLength": 1,
          "type": "string"
        },
        "updated_by": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "variant_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "variant_id",
        "updated_by",
        "updated_at"
      ],
      "type": "object"
    },
    "signature": "\"(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]], MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "EvaluationReviewView",
  "unit": "export"
}
```
