# stove0_target_protocol.SemanticIntentConformanceVector

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticintentconf-7f964783b2:91733ad0b4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ec4e2f22e8"></a>
- <a id="s-08d23a3e57"></a>`distribution`: `stove0-target-protocol`
- <a id="s-6e0f6e3663"></a>`module`: `stove0_target_protocol`
- <a id="s-89c112b446"></a>`name`: `SemanticIntentConformanceVector`
- <a id="s-af57b88d2c"></a>`unit`: `export`

### Declared structure

- <a id="s-6738097e96"></a>`kind`: `"class"`
- <a id="s-372fba08a3"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, intent: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-732070400a"></a>

- <a id="s-7425697313"></a>`type`: `"object"`
- <a id="s-fc161d496f"></a>`additionalProperties`: `false`
- <a id="s-ed3d11eec8"></a>`required`: `["id","accepted","intent"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-76494e4f0f"></a>`accepted` | yes | type="boolean" |  |
| <a id="s-c10206aec8"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-45417c7724"></a>`intent` | yes | type="object"; additionalProperties=([JsonValue](#s-1bf21429a7)) |  |

##### Definitions

- [JsonValue](#s-1bf21429a7)

##### <a id="s-1bf21429a7"></a>definition `JsonValue`

- Accepts: any JSON value.

## Governing policies

- <a id="pa-6aa19f9ced"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticIntentConformanceVector`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b2872060f69b078064170fd6876c74e909aa6d17c6c93d31a8cbb8dc544d9a5a -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "accepted": {
          "type": "boolean"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "intent": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        }
      },
      "required": [
        "id",
        "accepted",
        "intent"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, intent: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "SemanticIntentConformanceVector",
  "unit": "export"
}
```

</details>
