# STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-nvenc-av1-opus-target-zstd:a633458eff -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/106`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD` — `configuration-environment:STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract

```json
{
  "consumers": [
    "stove0-nvenc-av1-opus-target"
  ],
  "name": "STOVE0_NVENC_AV1_OPUS_TARGET_ZSTD"
}
```
