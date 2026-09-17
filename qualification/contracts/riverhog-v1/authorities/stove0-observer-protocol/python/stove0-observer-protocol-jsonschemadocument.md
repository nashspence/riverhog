# stove0_observer_protocol.JsonSchemaDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-jsonschemadocument:8ead62f404 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a557197c15"></a>
- <a id="s-a0777ab8aa"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-6396f3290b"></a>`module`: `stove0_observer_protocol`
- <a id="s-8bf4a9dc96"></a>`name`: `JsonSchemaDocument`
- <a id="s-9c03c2d302"></a>`unit`: `export`

### Declared structure

- <a id="s-58514e2c06"></a>`kind`: `"class"`
- <a id="s-75924628f7"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-770f88fa45"></a>

- <a id="s-60a4f94c9b"></a>`type`: `"object"`
- <a id="s-a03bd2d00a"></a>`additionalProperties`: `false`
- <a id="s-021153d0b4"></a>`required`: `["id","sha256","schema"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0f909fb2ab"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-70eb458230"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-b5de131dc4"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e8b8df0b0d"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-9b40aa03df)) |  |
| <a id="s-0510a15fa9"></a>`sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Definitions

- [JsonValue](#s-9b40aa03df)

##### <a id="s-9b40aa03df"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [verify_digest](stove0-observer-protocol-jsonschemadocument-verify-digest.md)
- [from_schema](stove0-observer-protocol-jsonschemadocument-from-schema.md)

## Governing policies

- <a id="pa-79d97074d5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.JsonSchemaDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6ae8ec0b60fbecb8e12bf3bbe31d154549ea2ecdc23d463da64929a0c004404f -->

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
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "JsonSchemaDocument",
  "unit": "export"
}
```

</details>
