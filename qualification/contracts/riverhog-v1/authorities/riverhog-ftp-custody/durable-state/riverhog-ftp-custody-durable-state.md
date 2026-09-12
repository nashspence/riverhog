# riverhog-ftp-custody durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-ftp-custody:riverhog-ftp-custody-durable-state:319b9df697 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-custody` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/6`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:riverhog-ftp-custody` — `state:riverhog-ftp-custody`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract summary

- `format`: riverhog-ftp-adapter-claim/v1

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a1cdcddcc2603b2a8b9a3d31106e7a4f808cf1c62f3ff7a5e45b180e9deee710 -->

```json
{
  "distribution": "riverhog-ftp-adapter",
  "fixture_sha256s": [
    "4ade71a24a784d4893d8f447fedecbab4cdde5257d21e174b2ccd654027be95f",
    "565b24bc77ebeee74f70f6c608e099956666c3589ed85146fcea7e77d9f25356"
  ],
  "format": "riverhog-ftp-adapter-claim/v1",
  "head": "v1",
  "id": "riverhog-ftp-custody"
}
```
