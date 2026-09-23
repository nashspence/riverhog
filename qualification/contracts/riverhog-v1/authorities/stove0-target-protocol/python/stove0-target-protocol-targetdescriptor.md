# stove0_target_protocol.TargetDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptor:08dad052cd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c361b2fcce"></a>
- <a id="s-c12d77b4db"></a>`distribution`: `stove0-target-protocol`
- <a id="s-c0810c0a6a"></a>`module`: `stove0_target_protocol`
- <a id="s-d76ec7a612"></a>`name`: `TargetDescriptor`
- <a id="s-2bbdea791c"></a>`unit`: `export`

### Declared structure

- <a id="s-dfd1b8ddba"></a>`kind`: `"class"`
- <a id="s-58cf34304e"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-080a965cc2"></a>

- <a id="s-4b9a51005c"></a>`type`: `"object"`
- <a id="s-c992a126db"></a>`additionalProperties`: `false`
- <a id="s-ba88276fb5"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","operations","descriptor_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a59eab2e2d"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9bfc1cfff3"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-7b4918d32b"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-644bbb9fa3"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-962ebb9a09"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-975ad9d451)); minItems=1 |  |
| <a id="s-05ab938425"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-4ba0522c42"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-2d22d284a1"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-1942e258fa)
- [JsonValue](#s-c56f3ca278)
- [TargetOperationSupport](#s-975ad9d451)

##### <a id="s-1942e258fa"></a>definition `JsonSchemaValidationProfile`

- <a id="s-f771fa95d3"></a>`type`: `"object"`
- <a id="s-c238d731bf"></a>`additionalProperties`: `false`
- <a id="s-11cfc843cb"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dedabfa416"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-d4ac9322c2"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-13bbb0460c"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-2b7e98aa8f"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-a8c4463a80"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-c56f3ca278)) |  |

##### <a id="s-c56f3ca278"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-975ad9d451"></a>definition `TargetOperationSupport`

- <a id="s-879712ec92"></a>`type`: `"object"`
- <a id="s-81bb4aa90d"></a>`additionalProperties`: `false`
- <a id="s-a9a6221a5b"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d70b8feaf8"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-21972272b1"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-599ba13855"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-1942e258fa) |  |
| <a id="s-e3da50137f"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

## Maintained corroboration

### Related interface records

- [bind_result_kind](stove0-target-protocol-targetdescriptor-bind-result-kind.md)
- [canonical_operations](stove0-target-protocol-targetdescriptor-canonical-operations.md)
- [seal](stove0-target-protocol-targetdescriptor-seal.md)
- [support_for](stove0-target-protocol-targetdescriptor-support-for.md)
- [verify_digest](stove0-target-protocol-targetdescriptor-verify-digest.md)

## Governing policies

- <a id="pa-94713a56dd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ff8123b00fe2d10c7839700fa73cadb8738b3a72ed2be556c24a4617218144c -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonSchemaValidationProfile": {
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
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "profile_sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "TargetOperationSupport": {
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
              "$ref": "#/$defs/JsonSchemaValidationProfile"
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
        }
      },
      "additionalProperties": false,
      "properties": {
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "type": "string"
        },
        "operations": {
          "items": {
            "$ref": "#/$defs/TargetOperationSupport"
          },
          "minItems": 1,
          "type": "array"
        },
        "protocol": {
          "default": "stove0-transform-target/v1",
          "enum": [
            "stove0-transform-target/v1",
            "stove0-effect-target/v1"
          ],
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "type": "string"
        },
        "transport": {
          "const": "riverhog-capability/v1",
          "default": "riverhog-capability/v1",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_id",
        "operations",
        "descriptor_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TargetDescriptor",
  "unit": "export"
}
```

</details>
