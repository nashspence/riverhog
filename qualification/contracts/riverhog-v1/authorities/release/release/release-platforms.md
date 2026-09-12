# Release platforms

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-platforms:d8a16a7667 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `release-contract` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `end_user_artifacts` | ["linux-x64","macos-arm64","windows-x64"] |
| `runtime_images` | ["linux/amd64"] |

## Governing policies

- `compatibility/components/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/external_contract/release/platforms`

### Exact owned JSON

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
