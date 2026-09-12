# Release platforms

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-platforms:d8a16a7667 -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `release-contract` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/platforms`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `end_user_artifacts` | array (3 items) |
| `runtime_images` | array (1 items) |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e16534e00d354ad33a8fd8bbc75ac109eeac3a9f7c45fb8e20277d7fdb3f12ca -->

```json
{
  "end_user_artifacts": [
    "linux-x64",
    "macos-arm64",
    "windows-x64"
  ],
  "runtime_images": [
    "linux/amd64"
  ]
}
```
