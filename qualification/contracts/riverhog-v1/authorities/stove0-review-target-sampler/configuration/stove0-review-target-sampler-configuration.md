# stove0-review-target-sampler configuration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration:stove0-review-target-sampler:stove0-review-target-sampler-configuration:f34c0cad9c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-review-target-sampler` |
| Interface | `configuration` |
| Family | `documents` |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- `title`: SamplerConfig
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `allow_insecure_http` | no | type="boolean" |  |
| `base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| `descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| `image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| `token_file` | yes | type="string"; format="path" |  |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `contract_max` | maximum=2048, minimum=1, reason=schema-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Governing policies

- `compatibility/configuration/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration:stove0-review-target-sampler` — `reference/stove0/targets/review/support/src/stove0_review_target_support/app.py::SamplerConfig`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_documents/stove0-review-target-sampler`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f41a6e541ec173a2e5d15cf28386e186b3440a8e7345c69f22509c6add3dad8e -->

```json
{
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
```
