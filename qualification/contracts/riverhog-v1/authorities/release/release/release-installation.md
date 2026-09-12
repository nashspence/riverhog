# Release installation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-installation:63e13a1ed0 -->

| Audit field | Value |
|---|---|
| Authority | `release` |
| Interface | `release` |
| Family | `release-contract` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/release/installation`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

```json
{
  "listener": {
    "autorun": "explicit-required",
    "provider_identity": "persisted-exactly",
    "provider_selection": "explicit",
    "resume": "next-login",
    "root": "gogurt",
    "scope": "current-user"
  },
  "lock_format": "pylock.toml",
  "managed_python": true,
  "method": "uv-tool",
  "roots": [
    "gogurt",
    "piggity",
    "riverhog-recover",
    "stove0-client"
  ],
  "simple_index_path": "artifacts/v{version}/simple/",
  "wheel_only": true
}
```
