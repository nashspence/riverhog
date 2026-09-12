# gogurt.listener-host-providers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-listener-runtime:gogurt-listener-host-providers:2a70aa60e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener-runtime` |
| Interface | `boundary` |
| Family | `entry-point-extensions` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `group` | "gogurt.listener-host-providers" |
| `owner` | "gogurt-listener-runtime" |
| `owner_constant` | "GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP" |
| `owner_path` | "reference/gogurt/packages/listener-runtime" |
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

- `/boundaries/entry_point_extensions/0`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a037abeaa91a8c58f20dd4ad0b0c473111587f48b1fb4a89d9691111cc4fc5dd -->

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
