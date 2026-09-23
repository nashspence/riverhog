# stove0_observer_protocol.ObserverDescriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescriptor:0d772f46fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ea4ccf8767"></a>
- <a id="s-60e459675a"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-cc57cdf4e9"></a>`module`: `stove0_observer_protocol`
- <a id="s-f4f7a54b0d"></a>`name`: `ObserverDescriptor`
- <a id="s-e37aa6e119"></a>`unit`: `export`

### Declared structure

- <a id="s-ec06d4330d"></a>`kind`: `"class"`
- <a id="s-e44eabb9f9"></a>`signature`: `"\"(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-76460c69d0"></a>

- <a id="s-7fd1a8986b"></a>`type`: `"object"`
- <a id="s-0fe06f87bf"></a>`additionalProperties`: `false`
- <a id="s-314c22ff73"></a>`required`: `["implementation_id","implementation_version","source_revision","image_id","contracts","descriptor_sha256"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbfda6966c"></a>`contracts` | yes | type="array"; items=([ObserverContractSupport](#s-8508ad0f92)); minItems=1 |  |
| <a id="s-792e058da3"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-5a06e12d85"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$" |  |
| <a id="s-fd38d5855d"></a>`implementation_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ddca7aa3e3"></a>`implementation_version` | yes | type="string"; maxLength=120; minLength=1 |  |
| <a id="s-f4542ba322"></a>`protocol` | no | type="string"; const="stove0-content-observer/v1"; default="stove0-content-observer/v1" |  |
| <a id="s-34e3494bae"></a>`source_revision` | yes | type="string"; maxLength=200; minLength=1 |  |

##### Definitions

- [JsonSchemaValidationProfile](#s-ed7e72cde4)
- [JsonValue](#s-0d91f96b71)
- [ObserverContractSupport](#s-8508ad0f92)
- [SemanticValidationProfile](#s-3e7f407eeb)

##### <a id="s-ed7e72cde4"></a>definition `JsonSchemaValidationProfile`

- <a id="s-1d62664f9b"></a>`type`: `"object"`
- <a id="s-7fb47340a7"></a>`additionalProperties`: `false`
- <a id="s-ac0457bea0"></a>`required`: `["id","profile_sha256","schema"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-448dc8697e"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-a018637be9"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-466eee6eea"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-b62abe4af2"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9bde3daa88"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-0d91f96b71)) |  |

##### <a id="s-0d91f96b71"></a>definition `JsonValue`

- Accepts: any JSON value.

##### <a id="s-8508ad0f92"></a>definition `ObserverContractSupport`

- <a id="s-8e2cc080b2"></a>`type`: `"object"`
- <a id="s-560cc63e15"></a>`additionalProperties`: `false`
- <a id="s-48d78d924f"></a>`required`: `["contract_id","contract_sha256","options_schema","facts_schema","facts_semantics","maximum_result_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25103029b6"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-ef80d92142"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-92db38b820"></a>`facts_schema` | yes | [JsonSchemaValidationProfile](#s-ed7e72cde4) |  |
| <a id="s-790996af2b"></a>`facts_semantics` | yes | [SemanticValidationProfile](#s-3e7f407eeb) |  |
| <a id="s-9b4d52ccd7"></a>`maximum_result_bytes` | yes | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-0e45ea61ea"></a>`options_schema` | yes | [JsonSchemaValidationProfile](#s-ed7e72cde4) |  |
| <a id="s-e36695e079"></a>`preferred_subject_batch_size` | no | type="integer"; minimum=1; default=128 |  |

##### <a id="s-3e7f407eeb"></a>definition `SemanticValidationProfile`

- <a id="s-5073ef5da5"></a>`type`: `"object"`
- <a id="s-0c82956c6f"></a>`additionalProperties`: `false`
- <a id="s-788633e11f"></a>`required`: `["id","rules","profile_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7f92cb1935"></a>`conformance_vectors_sha256` | no | anyOf=[(type="string"; pattern="^[0-9a-f]{64}$"); (type="null")]; default=null |  |
| <a id="s-a256087471"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-928f4ee935"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-9e85ac85ab"></a>`rules` | yes | type="array"; items=(type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$"); minItems=1 |  |

## Maintained corroboration

### Related interface records

- [unique_contracts](stove0-observer-protocol-observerdescriptor-unique-contracts.md)
- [verify_digest](stove0-observer-protocol-observerdescriptor-verify-digest.md)
- [support_for](stove0-observer-protocol-observerdescriptor-support-for.md)
- [seal](stove0-observer-protocol-observerdescriptor-seal.md)

## Governing policies

- <a id="pa-3956c6da1e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f6726403c26231efd78f020ccb133ac158cd6c9e34718d6f0b77785a0dc79dd -->

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
        "ObserverContractSupport": {
          "additionalProperties": false,
          "properties": {
            "contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "facts_schema": {
              "$ref": "#/$defs/JsonSchemaValidationProfile"
            },
            "facts_semantics": {
              "$ref": "#/$defs/SemanticValidationProfile"
            },
            "maximum_result_bytes": {
              "maximum": 67108864,
              "minimum": 1,
              "type": "integer"
            },
            "options_schema": {
              "$ref": "#/$defs/JsonSchemaValidationProfile"
            },
            "preferred_subject_batch_size": {
              "default": 128,
              "minimum": 1,
              "type": "integer"
            }
          },
          "required": [
            "contract_id",
            "contract_sha256",
            "options_schema",
            "facts_schema",
            "facts_semantics",
            "maximum_result_bytes"
          ],
          "type": "object"
        },
        "SemanticValidationProfile": {
          "additionalProperties": false,
          "properties": {
            "conformance_vectors_sha256": {
              "anyOf": [
                {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "contracts": {
          "items": {
            "$ref": "#/$defs/ObserverContractSupport"
          },
          "minItems": 1,
          "type": "array"
        },
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
        "protocol": {
          "const": "stove0-content-observer/v1",
          "default": "stove0-content-observer/v1",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_id",
        "contracts",
        "descriptor_sha256"
      ],
      "type": "object"
    },
    "signature": "\"(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^sha256:[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "ObserverDescriptor",
  "unit": "export"
}
```

</details>
