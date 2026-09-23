# stove0_target_support.TargetFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetfailure:09ce8d2403 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dce584700f"></a>
- <a id="s-f66d8a8288"></a>`distribution`: `stove0-target-support`
- <a id="s-7184615570"></a>`module`: `stove0_target_support`
- <a id="s-ded8318de1"></a>`name`: `TargetFailure`
- <a id="s-c5ab76ee59"></a>`unit`: `export`

### Declared structure

- <a id="s-6c64612257"></a>`kind`: `"class"`
- <a id="s-39c0cc7fa8"></a>`signature`: `"\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""`

#### Validated model schema

<a id="s-dffb976f63"></a>

- <a id="s-25c95c2144"></a>`type`: `"object"`
- <a id="s-cf8f414c9d"></a>`additionalProperties`: `false`
- <a id="s-0999dfa498"></a>`required`: `["code","message","retryable"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7b9580a80a"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-938353d745"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-24486c1a4a"></a>`retryable` | yes | type="boolean" |  |

## Governing policies

- <a id="pa-46bcc9ff56"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetFailure`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0b9659d707b5fd74c4989e8c0742049a9f114fc8a42fcd3e342073a9a29c1b4 -->

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
        },
        "retryable": {
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "type": "object"
    },
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetFailure",
  "unit": "export"
}
```

</details>
