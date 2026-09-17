# stove0_core.EvaluationReview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-evaluationreview:a878834366 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a1d072f335"></a>
- <a id="s-d6ef84d860"></a>`distribution`: `stove0-server`
- <a id="s-2bd2b6be8a"></a>`module`: `stove0_core`
- <a id="s-ebf4a78d89"></a>`name`: `EvaluationReview`
- <a id="s-ee558774f9"></a>`unit`: `export`

### Declared structure

- <a id="s-00425a4db9"></a>`kind`: `"class"`
- <a id="s-e30af7ae1a"></a>`signature`: `"'(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int \| None, Ge(ge=1), Le(le=5)] = None, note: Annotated[str \| None, MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None'"`

#### Validated model schema

<a id="s-a4aaaab6d9"></a>

- <a id="s-486af52e6d"></a>`type`: `"object"`
- <a id="s-e58aad16f7"></a>`additionalProperties`: `false`
- <a id="s-5d0795774b"></a>`required`: `["variant_id","updated_by","updated_at"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-86cd0fbc39"></a>`note` | no | anyOf=[(type="string"; maxLength=4000); (type="null")]; default=null |  |
| <a id="s-1925280957"></a>`rating` | no | anyOf=[(type="integer"; minimum=1; maximum=5); (type="null")]; default=null |  |
| <a id="s-33963925a9"></a>`updated_at` | yes | type="string"; maxLength=40; minLength=1 |  |
| <a id="s-4bcd006ee3"></a>`updated_by` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-a03c66e0ad"></a>`variant_id` | yes | type="string"; maxLength=160; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [meaningful](stove0-core-evaluationreview-meaningful.md)

## Governing policies

- <a id="pa-52a3cc2577"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.EvaluationReview`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b209d05bca137e3412e3fcd009811f303db2117343f5913167b271e1db93137 -->

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
    "signature": "'(*, variant_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], rating: Annotated[int | None, Ge(ge=1), Le(le=5)] = None, note: Annotated[str | None, MaxLen(max_length=4000)] = None, updated_by: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], updated_at: Annotated[str, MinLen(min_length=1), MaxLen(max_length=40)]) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EvaluationReview",
  "unit": "export"
}
```

</details>
