# stove0-ffprobe-sampling-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-component-boundary:cfad371240 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-ffprobe-sampling-observer](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-1edd329d34) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7d2463df29"></a>
| Field | Shape |
|---|---|
| <a id="s-2fef91ec71"></a>`console_scripts` | additional keys=`stove0-ffprobe-sampling-observer` |
| <a id="s-d830ceb347"></a>`dependencies` | ["http-api-contracts","stove0-media-sampling-observer-contracts","stove0-observer-protocol","stove0-observer-support"] |
| <a id="s-e7aef5c235"></a>`distribution` | "stove0-ffprobe-sampling-observer" |
| <a id="s-87f62656dd"></a>`optional_dependencies` | empty object |
| <a id="s-508007e605"></a>`path` | "reference/stove0/observers/ffprobe-sampling" |
| <a id="s-a1df3176c6"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-36b0fee5c7"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/45`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 02a18b2c9ecd5777269d1183e2471301ce8871471a62f33fec09b4bfa681af72 -->

```json
{
  "console_scripts": {
    "stove0-ffprobe-sampling-observer": "stove0_ffprobe_sampling_observer.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "stove0-media-sampling-observer-contracts",
    "stove0-observer-protocol",
    "stove0-observer-support"
  ],
  "distribution": "stove0-ffprobe-sampling-observer",
  "optional_dependencies": {},
  "path": "reference/stove0/observers/ffprobe-sampling",
  "role": "reference_component"
}
```
