# stove0_target_support.TargetContract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontract:8d6698de0c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-26b04616e3"></a>
- <a id="s-0e0b9c9e8c"></a>`distribution`: `stove0-target-support`
- <a id="s-eb55c8879f"></a>`module`: `stove0_target_support`
- <a id="s-50f64be519"></a>`name`: `TargetContract`
- <a id="s-959cd9c417"></a>`unit`: `export`

### Declared structure

- <a id="s-39ef0533d8"></a>`kind`: `"class"`
- <a id="s-5d34edcfb5"></a>`signature`: `"\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-3fbf3451b8"></a>

- <a id="s-85c55d120f"></a>`type`: `"object"`
- <a id="s-099a9d3b0d"></a>`additionalProperties`: `false`
- <a id="s-a014d6d7e7"></a>`required`: `["implementation_id","implementation_version","source_revision","image_digest","operations","contract_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3eee0df551"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-96fdbbe108"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f64f1e6081"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-577410a5a4"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-3bdbdb5a14"></a>`operations` | yes | type="array"; items=([TargetOperationSupport](#s-62e5485c60)); minItems=1 |  |
| <a id="s-26595c4fed"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"]; default="stove0-transform-target/v1" |  |
| <a id="s-1253af1a22"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |
| <a id="s-98a51df6e1"></a>`transport` | no | type="string"; const="riverhog-capability/v1"; default="riverhog-capability/v1" |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-b7a748632e)
- [JsonValue](#s-6ed39a772b)
- [TargetOperationSupport](#s-62e5485c60)

##### <a id="s-b7a748632e"></a>definition `JsonSchemaValidationProfile`

- <a id="s-8861ed4496"></a>`type`: `"object"`
- <a id="s-01e1aed421"></a>`additionalProperties`: `false`
- <a id="s-22342776b6"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2b80372b39"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-f616e6cc16"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-245f87f1b3"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-bdea35f65f"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-6edf52c5f6"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-6ed39a772b)) |  |

##### <a id="s-6ed39a772b"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-62e5485c60"></a>definition `TargetOperationSupport`

- <a id="s-f765606c4f"></a>`type`: `"object"`
- <a id="s-6c73f05fc1"></a>`additionalProperties`: `false`
- <a id="s-895c67bdab"></a>`required`: `["operation_id","operation_contract_sha256","options_schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4763d0d08f"></a>`operation_contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e213ab93ca"></a>`operation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-165ca39b4c"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-b7a748632e) |  |
| <a id="s-8e22901001"></a>`result_kind` | no | type="string"; enum=["collection","external-effect"]; default="collection" |  |

## Maintained corroboration

### Related interface records

- [bind_result_kind](stove0-target-support-targetcontract-bind-result-kind.md)
- [canonical_operations](stove0-target-support-targetcontract-canonical-operations.md)
- [seal](stove0-target-support-targetcontract-seal.md)
- [support_for](stove0-target-support-targetcontract-support-for.md)
- [verify_digest](stove0-target-support-targetcontract-verify-digest.md)

## Governing policies

- <a id="pa-8a60bc62b7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a207c237518d2357e0667695ac5946756e9d0eb1186c7182d9556711fcb046e7 -->

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
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
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
        "image_digest",
        "operations",
        "contract_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetContract",
  "unit": "export"
}
```

</details>
