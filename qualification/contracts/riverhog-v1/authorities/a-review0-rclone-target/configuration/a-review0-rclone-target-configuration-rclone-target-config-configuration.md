# a-review0-rclone-target:configuration:rclone-target-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:a-review0-rclone-target:a-review0-rclone-target-configuration-rcl-fa9ce3d16d:2f1179e641 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-4db9bac1a6"></a>

- <a id="s-f9193213dc"></a>`type`: `"object"`
- <a id="s-95a3dc6782"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-review0-rclone-target.schema.json"`
- <a id="s-f5b2ea565f"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-a9450a12f8"></a>`additionalProperties`: `false`
- <a id="s-7a16ae30d5"></a>`required`: `["token_file","samplers","destination_identity","rclone_remote"]`
- <a id="s-b7ed9e3e1f"></a>`title`: `"RcloneTargetConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2f5a15889a"></a>`destination_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Destination Identity" |  |
| <a id="s-c00fe8f8ac"></a>`rclone_config_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Rclone Config File" |  |
| <a id="s-288ea7e6ab"></a>`rclone_remote` | yes | type="string"; minLength=1; title="Rclone Remote" |  |
| <a id="s-b69f6d77ed"></a>`rclone_timeout_seconds` | no | type="integer"; minimum=1; default=86400; title="Rclone Timeout Seconds" |  |
| <a id="s-74b82d3d4c"></a>`samplers` | yes | type="array"; items=([SamplerConfig](#s-47876ef454)); minItems=1; title="Samplers" |  |
| <a id="s-dece63b79e"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Definitions

- [SamplerConfig](#s-47876ef454)

### <a id="s-47876ef454"></a>definition `SamplerConfig`

- <a id="s-512d70a9e0"></a>`type`: `"object"`
- <a id="s-54f76d9076"></a>`additionalProperties`: `false`
- <a id="s-adbc515a03"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_id"]`
- <a id="s-f36a699ff3"></a>`title`: `"SamplerConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2611728baf"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-3b801e1792"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Base Url" |  |
| <a id="s-e39e6f4c57"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-1b95de3b59"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-77c971e8c5"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$"; title="Image Id" |  |
| <a id="s-c6ca6d7288"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-review0-rclone-target:configuration:rclone-target-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field samplers](#s-74b82d3d4c) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SamplerConfig · field base_url](#s-3b801e1792) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [definition SamplerConfig · field descriptor_sha256](#s-e39e6f4c57) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field destination_identity](#s-2f5a15889a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f4faefab54"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-657061dcbc"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-d5d4c9717c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:a-review0-rclone-target:configuration:rclone-target-config](../../../evidence/sources/authorities.md#src-c5f915af22) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/app.py::RcloneTargetConfig](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/a-review0-rclone-target:configuration:rclone-target-config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efe5a166d9033c4a7c463557eb478a93555a11a6e7df79e5f4281f1f297011ba -->

```json
{
  "$defs": {
    "SamplerConfig": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Base Url",
          "type": "string"
        },
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "image_id": {
          "pattern": "^sha256:[0-9a-f]{64}$",
          "title": "Image Id",
          "type": "string"
        },
        "token_file": {
          "format": "path",
          "title": "Token File",
          "type": "string"
        }
      },
      "required": [
        "id",
        "base_url",
        "token_file",
        "descriptor_sha256",
        "image_id"
      ],
      "title": "SamplerConfig",
      "type": "object"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/config/a-review0-rclone-target.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "destination_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Destination Identity",
      "type": "string"
    },
    "rclone_config_file": {
      "anyOf": [
        {
          "format": "path",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "title": "Rclone Config File"
    },
    "rclone_remote": {
      "minLength": 1,
      "title": "Rclone Remote",
      "type": "string"
    },
    "rclone_timeout_seconds": {
      "default": 86400,
      "minimum": 1,
      "title": "Rclone Timeout Seconds",
      "type": "integer"
    },
    "samplers": {
      "items": {
        "$ref": "#/$defs/SamplerConfig"
      },
      "minItems": 1,
      "title": "Samplers",
      "type": "array"
    },
    "token_file": {
      "format": "path",
      "title": "Token File",
      "type": "string"
    }
  },
  "required": [
    "token_file",
    "samplers",
    "destination_identity",
    "rclone_remote"
  ],
  "title": "RcloneTargetConfig",
  "type": "object"
}
```

</details>
