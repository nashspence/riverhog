# stove0-ffprobe-sampling-observer component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-ffprobe-sampling-observer:stove0-ffprobe-sampling-observer-component-boundary:cfad371240 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-ffprobe-sampling-observer` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/45`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

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
