# gogurt.mounted-volume-providers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-core:gogurt-mounted-volume-providers:996696876b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-core` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `group` | "gogurt.mounted-volume-providers" |
| `owner` | "gogurt-core" |
| `owner_constant` | "GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP" |
| `owner_path` | "reference/gogurt/packages/core" |
| `providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

### Machine authority

- `/boundaries/entry_point_extensions/1`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 013b7c8cf9f3dec34750c3568d2377c0a18ed2e6dcacc38c2a6b1818c2a6d20c -->

```json
{
  "group": "gogurt.mounted-volume-providers",
  "owner": "gogurt-core",
  "owner_constant": "GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP",
  "owner_path": "reference/gogurt/packages/core",
  "providers": [
    {
      "distribution": "gogurt-linux-mounted-volume",
      "name": "gogurt-linux-mounted-volume",
      "value": "gogurt_linux_mounted_volume:MOUNTED_VOLUME_PROVIDER_BINDING"
    },
    {
      "distribution": "gogurt-macos-mounted-volume",
      "name": "gogurt-macos-mounted-volume",
      "value": "gogurt_macos_mounted_volume:MOUNTED_VOLUME_PROVIDER_BINDING"
    },
    {
      "distribution": "gogurt-windows-mounted-volume",
      "name": "gogurt-windows-mounted-volume",
      "value": "gogurt_windows_mounted_volume:MOUNTED_VOLUME_PROVIDER_BINDING"
    }
  ]
}
```
