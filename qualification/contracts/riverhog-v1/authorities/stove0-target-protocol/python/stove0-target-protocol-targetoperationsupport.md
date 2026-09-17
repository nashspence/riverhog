# stove0_target_protocol.TargetOperationSupport

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetoperationsupport:b53f051474 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ece49ed72b"></a>
- <a id="s-fb8dec99b2"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d3dd75b48f"></a>`module`: `stove0_target_protocol`
- <a id="s-b261ef37af"></a>`name`: `TargetOperationSupport`
- <a id="s-058a6a2cb6"></a>`unit`: `export`

### Declared structure

- <a id="s-fde8de99ad"></a>`kind`: `"class"`
- <a id="s-3a10ce622c"></a>`signature`: `"\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaDocument) -> None\""`

#### Validated model schema

<a id="s-10716ebc9a"></a>

- <a id="s-6c21a79efd"></a>`type`: `"object"`
- <a id="s-65b19975dd"></a>`additionalProperties`: `false`
- <a id="s-3c8fc80f86"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1925f23f92"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-23db595668"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7487ad020a"></a>`options_schema` | yes | [JsonSchemaDocument](#s-25b3b49058) |  |
| <a id="s-31423e02bf"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

##### Definitions

- [JsonSchemaDocument](#s-25b3b49058)
- [JsonValue](#s-8a10c7d7ba)

##### <a id="s-25b3b49058"></a>definition `JsonSchemaDocument`

- <a id="s-1387f79897"></a>`type`: `"object"`
- <a id="s-3d9eaecb58"></a>`additionalProperties`: `false`
- <a id="s-88b865d3c0"></a>`required`: `["id","sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1589ae209d"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-e691c3cd9a"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-5853851f40"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-f4aef44046"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-8a10c7d7ba)) |  |
| <a id="s-3d50f67e20"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### <a id="s-8a10c7d7ba"></a>definition `JsonValue`

- Accepts: any JSON value.

## Governing policies

- <a id="pa-a56400b1e3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetOperationSupport`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c49291b7271400ec87ab463b97c9c8f339f48115bfe9691559523f9ed4bb4904 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonSchemaDocument": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "operation_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "operation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "result_kind": {
          "default": "collection",
          "enum": [
            "collection",
            "external-effect"
          ],
          "type": "string"
        }
      },
      "required": [
        "operation_id",
        "operation_contract_sha256",
        "options_schema"
      ],
      "type": "object"
    },
    "signature": "\"(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaDocument) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetOperationSupport",
  "unit": "export"
}
```

</details>
