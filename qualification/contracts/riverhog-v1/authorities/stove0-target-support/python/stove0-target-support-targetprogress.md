# stove0_target_support.TargetProgress

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetprogress:975516b022 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a51cad10c3"></a>
- <a id="s-96f9e89a7b"></a>`distribution`: `stove0-target-support`
- <a id="s-b08cb82556"></a>`module`: `stove0_target_support`
- <a id="s-9a5a7a434e"></a>`name`: `TargetProgress`
- <a id="s-58b9269cf2"></a>`unit`: `export`

### Declared structure

- <a id="s-e25e1365e3"></a>`kind`: `"class"`
- <a id="s-ec9d398b2d"></a>`signature`: `"'(*, phase: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], completed: Annotated[int, Ge(ge=0)], total: Annotated[int \| None, Ge(ge=0)] = None, unit: Annotated[str \| None, MinLen(min_length=1), MaxLen(max_length=40)] = None) -> None'"`

#### Validated model schema

<a id="s-e858ee88a0"></a>
- <a id="s-13a688ee83"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5bf7647948"></a>`completed` | yes | type="integer"; minimum=0 |  |
| <a id="s-3d99a15ede"></a>`phase` | yes | type="string"; minLength=1; maxLength=120 |  |
| <a id="s-b02d483182"></a>`total` | no | anyOf=type="integer"; minimum=0 \| type="null" |  |
| <a id="s-3ff3b017cf"></a>`unit` | no | anyOf=type="string"; minLength=1; maxLength=40 \| type="null" |  |

## Maintained corroboration

### Related interface records

- [stove0_target_support.TargetProgress.validate_total](stove0-target-support-targetprogress-validate-total.md)

## Governing policies

- <a id="pa-0504dd7528"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetProgress`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c1d70adcec515e655aae02e88b7128f3e23812e090a17b0caa9fcba37e5755e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "completed": {
          "minimum": 0,
          "type": "integer"
        },
        "phase": {
          "maxLength": 120,
          "minLength": 1,
          "type": "string"
        },
        "total": {
          "anyOf": [
            {
              "minimum": 0,
              "type": "integer"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "unit": {
          "anyOf": [
            {
              "maxLength": 40,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "phase",
        "completed"
      ],
      "type": "object"
    },
    "signature": "'(*, phase: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], completed: Annotated[int, Ge(ge=0)], total: Annotated[int | None, Ge(ge=0)] = None, unit: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None) -> None'"
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetProgress",
  "unit": "export"
}
```
