# RcloneTargetConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-review0-rclone-target:rclonetargetconfig:302e47e9c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-rclone-target](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-5365839dcd"></a>

- <a id="s-0238267119"></a>`type`: `"object"`
- <a id="s-6ef41c1cea"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-review0-rclone-target.schema.json"`
- <a id="s-ddcbc2f65e"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-aa919a4c6b"></a>`additionalProperties`: `false`
- <a id="s-de04742167"></a>`required`: `["token_file","samplers","destination_identity","rclone_remote"]`
- <a id="s-5ef818397b"></a>`title`: `"RcloneTargetConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-701e82775f"></a>`destination_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Destination Identity" |  |
| <a id="s-e3f01a6c8d"></a>`rclone_config_file` | no | anyOf=[(type="string"; format="path"); (type="null")]; default=null; title="Rclone Config File" |  |
| <a id="s-37179d31df"></a>`rclone_remote` | yes | type="string"; minLength=1; title="Rclone Remote" |  |
| <a id="s-2bdb02414e"></a>`rclone_timeout_seconds` | no | type="integer"; minimum=1; default=86400; title="Rclone Timeout Seconds" |  |
| <a id="s-cbe06057e4"></a>`samplers` | yes | type="array"; items=([SamplerConfig](#s-1295ad18ff)); minItems=1; title="Samplers" |  |
| <a id="s-6314b9e59d"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Definitions

- [SamplerConfig](#s-1295ad18ff)

### <a id="s-1295ad18ff"></a>definition `SamplerConfig`

- <a id="s-cc84f59e1f"></a>`type`: `"object"`
- <a id="s-a8779e0a49"></a>`additionalProperties`: `false`
- <a id="s-f7f31e2909"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_id"]`
- <a id="s-7b80753e32"></a>`title`: `"SamplerConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a1087ac04"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-b915214963"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Base Url" |  |
| <a id="s-f965272cc5"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-c98de346de"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-2d128777c5"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$"; title="Image Id" |  |
| <a id="s-aea0d6e75c"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/config/a-review0-rclone-target.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field samplers](#s-cbe06057e4) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SamplerConfig · field base_url](#s-b915214963) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [definition SamplerConfig · field descriptor_sha256](#s-f965272cc5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field destination_identity](#s-701e82775f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-6f686db690"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-996333f7b2"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-c32d8e8921"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/a-review0-rclone-target.schema.json](../../../evidence/sources/authorities.md#src-e6338ec203) — [some-implementations/stove0/review0/rclone-effect-target/src/a\_review0\_rclone\_target/config.schema.json](../../../../../../some-implementations/stove0/review0/rclone-effect-target/src/a_review0_rclone_target/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1a-review0-rclone-target.schema.json`

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
