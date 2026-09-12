# gogurt.listener-host-providers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-listener-runtime:gogurt-listener-host-providers:2a70aa60e4 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener-runtime` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/entry_point_extensions/0`

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
  "group": "gogurt.listener-host-providers",
  "owner": "gogurt-listener-runtime",
  "owner_constant": "GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP",
  "owner_path": "reference/gogurt/packages/listener-runtime",
  "providers": [
    {
      "distribution": "gogurt-linux-listener-host",
      "name": "gogurt-linux-listener-host",
      "value": "gogurt_linux_listener_host:LISTENER_HOST_PROVIDER_BINDING"
    },
    {
      "distribution": "gogurt-macos-listener-host",
      "name": "gogurt-macos-listener-host",
      "value": "gogurt_macos_listener_host:LISTENER_HOST_PROVIDER_BINDING"
    },
    {
      "distribution": "gogurt-windows-listener-host",
      "name": "gogurt-windows-listener-host",
      "value": "gogurt_windows_listener_host:LISTENER_HOST_PROVIDER_BINDING"
    }
  ]
}
```
