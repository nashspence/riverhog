# stove0-target-jobs durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-durable-state:2b6eb6272b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-target-jobs` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `format`: stove0-target-job-state/v1

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make release-check`
- `make database-qualification`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:stove0-target-jobs` — `state:stove0-target-jobs`

### Machine authority

- `/external_contract/durable_state/owners/5`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 992a5eda0a9f9740290e3465bcbfb0920e19b6d51ea8043d7b42ae783a3b3f60 -->

```json
{
  "distribution": "stove0-target-support",
  "fixture_sha256s": [
    "29254d1dc7d6429b8368f311c1d25fee6bf059f55d5f1b9e309e507659920cf7",
    "626cdf4d36eedf18fcd596fb81a6c49ea1fea98845f773ec33f8dd0b00887071",
    "a7a558f503adc7905717e7a99404bc42132f55e639bcb56cf0d194c4058dd4fc",
    "dbb18aa80a2e9e15f6dacf3194928b42411aea66004bb20663ab90749ad8e33b"
  ],
  "format": "stove0-target-job-state/v1",
  "head": "v1",
  "id": "stove0-target-jobs"
}
```
