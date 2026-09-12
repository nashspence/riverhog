# gogurt.mounted-volume-providers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-core:gogurt-mounted-volume-providers:996696876b -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-core` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/entry_point_extensions/1`

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
