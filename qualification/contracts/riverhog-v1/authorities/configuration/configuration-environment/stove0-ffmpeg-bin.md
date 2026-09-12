# STOVE0_FFMPEG_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-ffmpeg-bin:4d8a7bb05d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `consumers` | ["stove0-nvenc-av1-opus-review-sampler","stove0-nvenc-av1-opus-target","stove0-opus-review-sampler","stove0-opus-target"] |
| `name` | "STOVE0_FFMPEG_BIN" |

## Governing policies

- `compatibility/configuration/v1`

## Evidence

### Qualification

- `make unit`
- `make compose-smoke`

### Executable sources

- `configuration-environment:STOVE0_FFMPEG_BIN` — `configuration-environment:STOVE0_FFMPEG_BIN`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/95`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 413f1a9293c253aeb8b41eb3dcc1501edcd3bc1265a39ef4b2571b643c7da447 -->

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-review-sampler",
    "stove0-nvenc-av1-opus-target",
    "stove0-opus-review-sampler",
    "stove0-opus-target"
  ],
  "name": "STOVE0_FFMPEG_BIN"
}
```
