# ReviewTargetConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:a-review0-materializer:reviewtargetconfig:dd81a79d38 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-review0-materializer](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-fbd993aee8"></a>

- <a id="s-143591d1f3"></a>`type`: `"object"`
- <a id="s-016a5b7442"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/config/a-review0-materializer.schema.json"`
- <a id="s-78ab689aaf"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-ea25ef7079"></a>`additionalProperties`: `false`
- <a id="s-b85918cac1"></a>`required`: `["token_file","samplers"]`
- <a id="s-068c4b8a79"></a>`title`: `"ReviewTargetConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-05dfac1443"></a>`samplers` | yes | type="array"; items=([SamplerConfig](#s-a12276b124)); minItems=1; title="Samplers" |  |
| <a id="s-4534085b68"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Definitions

- [SamplerConfig](#s-a12276b124)

### <a id="s-a12276b124"></a>definition `SamplerConfig`

- <a id="s-bf06bf3db1"></a>`type`: `"object"`
- <a id="s-54811fed64"></a>`additionalProperties`: `false`
- <a id="s-4b6778ae24"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_id"]`
- <a id="s-c683b354e9"></a>`title`: `"SamplerConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b6dfdf3079"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-5d1a47d23e"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Base Url" |  |
| <a id="s-25efaa1570"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-d2784ce3ee"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-eb88153dd6"></a>`image_id` | yes | type="string"; pattern="^sha256:[0-9a-f]{64}$"; title="Image Id" |  |
| <a id="s-120332e3d7"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/config/a-review0-materializer.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field samplers](#s-05dfac1443) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SamplerConfig · field base_url](#s-5d1a47d23e) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [definition SamplerConfig · field descriptor_sha256](#s-25efaa1570) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-7d04cbe544"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-0a608ba207"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-6e1000b32b"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/config/a-review0-materializer.schema.json](../../../evidence/sources/authorities.md#src-06c06d5140) — [some-implementations/stove0/review0/materialize-target/src/a\_review0\_materializer/config.schema.json](../../../../../../some-implementations/stove0/review0/materialize-target/src/a_review0_materializer/config.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1config~1a-review0-materializer.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dcbeb2400fa5d3f23bda3236b18cbaa27307ef9f2abda4afe193a04ac1a2bdcc -->

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
  "$id": "https://nashspence.github.io/riverhog/v1/config/a-review0-materializer.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
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
    "samplers"
  ],
  "title": "ReviewTargetConfig",
  "type": "object"
}
```

</details>
