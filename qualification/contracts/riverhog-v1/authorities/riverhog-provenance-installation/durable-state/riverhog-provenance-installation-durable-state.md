# riverhog-provenance-installation durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-provenance-installation:riverhog-provenance-installation-durable-state:2a3ee53941 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-installation` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/7`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:riverhog-provenance-installation` — `state:riverhog-provenance-installation`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract summary

- `format`: riverhog-provenance-installation-id/v1

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3412629259ba1bf4fe27cb2a3ad8f05a05a153f52a8bd38ea9d52df974b50f32 -->

```json
{
  "distribution": "riverhog-provenance",
  "fixture_sha256s": [
    "fae5bd819bced353febb34ae9ad32e167f2522cebae4572e007f1747bc92277a"
  ],
  "format": "riverhog-provenance-installation-id/v1",
  "head": "v1",
  "id": "riverhog-provenance-installation"
}
```
