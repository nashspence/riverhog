# stove0-target-jobs durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-durable-state:2b6eb6272b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-jobs](../index.md) |
| Interface | [durable-state](index.md) |
| Family | [owners](index.md#f-b33bb60076a2) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b292d9d734e1"></a>
- <a id="s-b41f5a471012"></a>`format`: stove0-target-job-state/v1

## Governing policies

- <a id="pa-4a2231dc3d5e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make database-qualification](../../../evidence/sources.md#q-27f281b51ec6)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [state:stove0-target-jobs](../../../evidence/sources.md#src-7b4138829a38) — `state:stove0-target-jobs`

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
