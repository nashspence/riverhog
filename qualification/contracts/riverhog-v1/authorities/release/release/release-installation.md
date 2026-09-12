# Release installation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-installation:63e13a1ed0 -->

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
| `listener` | additional keys=`autorun`, `provider_identity`, `provider_selection`, `resume`, `root`, `scope` |
| `lock_format` | "pylock.toml" |
| `managed_python` | true |
| `method` | "uv-tool" |
| `roots` | ["gogurt","piggity","riverhog-recover","stove0-client"] |
| `simple_index_path` | "artifacts/v{version}/simple/" |
| `wheel_only` | true |

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

- `/external_contract/release/installation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 630504f7554a37df84f3b9370d2936a2f64a98eaef8bdc31f98b5e3eac0face8 -->

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
