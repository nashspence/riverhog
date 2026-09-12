# stove0-review-sampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-protocol:stove0-review-sampler:8e98df71df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [process-extensions](index.md#f-e38c5bdd179e) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3abe32ec5eaa"></a>
- <a id="s-8fb967b3f2b2"></a>`protocols`: ["stove0-review-sampler/v1"]

## Governing policies

- <a id="pa-9777613af393"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/process_extensions/2`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4841891cc9b850de788312cd1fbd692a1788dab12d2efdc77ee311a5379ecec0 -->

```json
{
  "binding": "http",
  "binding_support": "stove0-review-sampler-support",
  "binding_support_role": "reference_component",
  "contract_owner": "stove0-review-sampler-protocol",
  "contract_owner_role": "reference_component",
  "name": "stove0-review-sampler",
  "protocols": [
    "stove0-review-sampler/v1"
  ],
  "providers": [
    {
      "distribution": "stove0-nvenc-av1-opus-review-sampler",
      "images": [
        "stove0-nvenc-av1-opus-target"
      ]
    },
    {
      "distribution": "stove0-opus-review-sampler",
      "images": [
        "stove0-opus-target"
      ]
    }
  ],
  "schema_bundle_format": "stove0-review-sampler-schema-bundle/v1"
}
```
