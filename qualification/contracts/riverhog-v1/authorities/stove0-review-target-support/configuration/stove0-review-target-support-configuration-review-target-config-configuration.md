# stove0-review-target-support:configuration:review-target-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target-support:stove0-review-target-support-configuratio-1de58493fa:7e049e6756 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-d3e1aec762"></a>

- <a id="s-4280543e3c"></a>`type`: `"object"`
- <a id="s-0f1d00142d"></a>`additionalProperties`: `false`
- <a id="s-2e54db8d3b"></a>`required`: `["samplers"]`
- <a id="s-51615cd833"></a>`title`: `"ReviewTargetConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c32ef932b"></a>`samplers` | yes | type="array"; items=([SamplerConfig](#s-59188e1eee)); minItems=1; title="Samplers" |  |

### Definitions

- [SamplerConfig](#s-59188e1eee)

### <a id="s-59188e1eee"></a>definition `SamplerConfig`

- <a id="s-18d09066d0"></a>`type`: `"object"`
- <a id="s-535ec1e598"></a>`additionalProperties`: `false`
- <a id="s-b3f463d63d"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_digest"]`
- <a id="s-e8fcbe747e"></a>`title`: `"SamplerConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-682215731b"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-1cfe63b462"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Base Url" |  |
| <a id="s-377424141f"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-cd93604a6a"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-5f406f7876"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-4ebab6c7fe"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-review-target-support:configuration:review-target-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field samplers](#s-1c32ef932b) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SamplerConfig · field base_url](#s-1cfe63b462) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [definition SamplerConfig · field descriptor_sha256](#s-377424141f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerConfig · field image_digest](#s-5f406f7876) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-020c444442"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-091fe9ff09"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-57d941984f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:stove0-review-target-support:configuration:review-target-config](../../../evidence/sources/authorities.md#src-cca9387ce6) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/app.py::ReviewTargetConfig](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/stove0-review-target-support:configuration:review-target-config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 465ba4684267b5a403b655bcbaaa58dbe51e259b14ffa6dc2fbd902af816ff0c -->

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
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Image Digest",
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
        "image_digest"
      ],
      "title": "SamplerConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "samplers": {
      "items": {
        "$ref": "#/$defs/SamplerConfig"
      },
      "minItems": 1,
      "title": "Samplers",
      "type": "array"
    }
  },
  "required": [
    "samplers"
  ],
  "title": "ReviewTargetConfig",
  "type": "object"
}
```

</details>
