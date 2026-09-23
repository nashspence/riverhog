# review0-target-lib:configuration:review-target-config configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:review0-target-lib:review0-target-lib-configuration-review-t-4c9cf722b8:6b7843c288 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Configuration Documents](index.md) |

## External contract

<a id="s-a36460a25e"></a>

- <a id="s-9d34f79eb6"></a>`type`: `"object"`
- <a id="s-527636665f"></a>`additionalProperties`: `false`
- <a id="s-9d7ec3f827"></a>`required`: `["samplers"]`
- <a id="s-abe0a8b0da"></a>`title`: `"ReviewTargetConfig"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-386ccfd902"></a>`samplers` | yes | type="array"; items=([SamplerConfig](#s-6124ed84f7)); minItems=1; title="Samplers" |  |

### Definitions

- [SamplerConfig](#s-6124ed84f7)

### <a id="s-6124ed84f7"></a>definition `SamplerConfig`

- <a id="s-3c1ab11bee"></a>`type`: `"object"`
- <a id="s-4fc4308ae8"></a>`additionalProperties`: `false`
- <a id="s-1cb2659335"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_digest"]`
- <a id="s-4f0c1ba433"></a>`title`: `"SamplerConfig"`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c23c19cff1"></a>`allow_insecure_http` | no | type="boolean"; default=false; title="Allow Insecure Http" |  |
| <a id="s-ed763190bb"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1; title="Base Url" |  |
| <a id="s-690ef3edd6"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Descriptor Sha256" |  |
| <a id="s-92e96927c5"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$"; title="Id" |  |
| <a id="s-cbe1a1937a"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Image Digest" |  |
| <a id="s-cbd17d3c25"></a>`token_file` | yes | type="string"; format="path"; title="Token File" |  |

### Progression, limits, and lifecycle

#### [extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"review0-target-lib:configuration:review-target-config"}; maximum=null; reason="validated-deployment-composition"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field samplers](#s-386ccfd902) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition SamplerConfig · field base_url](#s-ed763190bb) | `length · characters · contract_max` | maximum=2048; minimum=1; reason="schema-maximum" |
| [definition SamplerConfig · field descriptor_sha256](#s-690ef3edd6) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition SamplerConfig · field image_digest](#s-cbe1a1937a) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-007fee93c3"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)
- <a id="pa-35f11b6a54"></a>[extent-rule/configuration-composition/v1](../../extent-contract/extent/extent-rule-configuration-composition.md#p-dcd344e8e5)
- <a id="pa-2ce60191b2"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration:review0-target-lib:configuration:review-target-config](../../../evidence/sources/authorities.md#src-50b3449542) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/app.py::ReviewTargetConfig](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/configuration_documents/review0-target-lib:configuration:review-target-config`

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
