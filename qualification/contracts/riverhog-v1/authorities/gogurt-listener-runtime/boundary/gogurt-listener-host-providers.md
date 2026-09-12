# gogurt.listener-host-providers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-listener-runtime:gogurt-listener-host-providers:2a70aa60e4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [boundary](index.md) |
| Family | [entry-point-extensions](index.md#f-4e3188b0a8) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e59c71699d"></a>
| Field | Shape |
|---|---|
| <a id="s-29c42ff47e"></a>`group` | "gogurt.listener-host-providers" |
| <a id="s-db9fa2586a"></a>`owner` | "gogurt-listener-runtime" |
| <a id="s-5a7ac764bd"></a>`owner_constant` | "GOGURT_LISTENER_HOST_PROVIDER_ENTRY_POINT_GROUP" |
| <a id="s-84dcf5cbc5"></a>`owner_path` | "reference/gogurt/packages/listener-runtime" |
| <a id="s-aa9d2e10c9"></a>`providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

## Governing policies

- <a id="pa-b961cccb5f"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

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
