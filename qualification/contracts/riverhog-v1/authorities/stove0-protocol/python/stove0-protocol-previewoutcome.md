# stove0_protocol.PreviewOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-previewoutcome:f3205c851d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9f2bbb4146"></a>
- <a id="s-77cec49fc7"></a>`distribution`: `stove0-protocol`
- <a id="s-97b6b20862"></a>`module`: `stove0_protocol`
- <a id="s-789d60d6d9"></a>`name`: `PreviewOutcome`
- <a id="s-f453d1dc75"></a>`unit`: `export`

### Declared structure

- <a id="s-d04d393373"></a>`kind`: `"class"`
- <a id="s-1712de18ee"></a>`signature`: `"\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool \| None = None) -> None\""`

#### Validated model schema

<a id="s-51109b7c24"></a>

- <a id="s-3863944cf4"></a>`type`: `"object"`
- <a id="s-a4bded7956"></a>`additionalProperties`: `false`
- <a id="s-dd99a3e8f8"></a>`required`: `["code","message"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2946b55f45"></a>`code` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-a7784c6ecf"></a>`message` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-7a03ef4cce"></a>`retryable` | no | anyOf=(type="boolean") \| (type="null"); default=null |  |

## Governing policies

- <a id="pa-10bdecf1bf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.PreviewOutcome`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2bef566d693425d9ac3d846ad397c4e73f4dbf37c7c8c9464d6b7e69336eab07 -->

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
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "code",
        "message"
      ],
      "type": "object"
    },
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool | None = None) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "PreviewOutcome",
  "unit": "export"
}
```

</details>
