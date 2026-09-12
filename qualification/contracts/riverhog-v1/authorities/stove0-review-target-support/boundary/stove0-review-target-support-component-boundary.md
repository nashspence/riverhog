# stove0-review-target-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-target-support:stove0-review-target-support-component-boundary:656c45b9d0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-a1982e9205) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-27bef53c02"></a>
| Field | Shape |
|---|---|
| <a id="s-cdb929b1ab"></a>`console_scripts` | empty object |
| <a id="s-7cfd5f8c67"></a>`dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","stove0-protocol","stove0-review-sampler-client","stove0-review-sampler-protocol","stove0-review-target-contracts","stove0-target-support"] |
| <a id="s-6cfb7d64ac"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-c260e2203b"></a>`optional_dependencies` | empty object |
| <a id="s-7754fd723d"></a>`path` | "reference/stove0/targets/review/support" |
| <a id="s-88afda9e75"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-817c66d510"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/69`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf7b12ec9a1179e1d5447669ac783a8a5d059a639d10b3a3a478425b5143896c -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "stove0-protocol",
    "stove0-review-sampler-client",
    "stove0-review-sampler-protocol",
    "stove0-review-target-contracts",
    "stove0-target-support"
  ],
  "distribution": "stove0-review-target-support",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/support",
  "role": "reference_component"
}
```
