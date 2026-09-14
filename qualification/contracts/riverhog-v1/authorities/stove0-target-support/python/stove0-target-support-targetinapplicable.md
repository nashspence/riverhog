# stove0_target_support.TargetInapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetinapplicable:3ce0796a3a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61b66a5f06"></a>
- <a id="s-414f32fe26"></a>`distribution`: `stove0-target-support`
- <a id="s-c7c2fcf16e"></a>`module`: `stove0_target_support`
- <a id="s-40291745d4"></a>`name`: `TargetInapplicable`
- <a id="s-f15f3122cc"></a>`unit`: `export`

### Declared structure

- <a id="s-cd33e0b6e2"></a>`kind`: `"class"`
- <a id="s-266f4900e5"></a>`signature`: `"\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""`

#### Validated model schema

<a id="s-44f4af96d6"></a>
- <a id="s-83c6b1c7ca"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c1cbc1e532"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cfe4975b35"></a>`message` | yes | type="string"; minLength=1; maxLength=1000 |  |

## Governing policies

- <a id="pa-bccdc7b03e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetInapplicable`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ebc28550a39d019760cdd5ff4634026abcfb79fb78508fe515d8b59c803e4d5 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "type": "object"
    },
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetInapplicable",
  "unit": "export"
}
```
