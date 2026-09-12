# gogurt.mounted-volume-providers

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-core:gogurt-mounted-volume-providers:996696876b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [boundary](index.md) |
| Family | [entry-point-extensions](index.md#f-ca9762d40d19) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3d2c759a5dc8"></a>
| Field | Shape |
|---|---|
| <a id="s-1b2e94dc6d8b"></a>`group` | "gogurt.mounted-volume-providers" |
| <a id="s-b32e1002e2cb"></a>`owner` | "gogurt-core" |
| <a id="s-849845cefb46"></a>`owner_constant` | "GOGURT_MOUNTED_VOLUME_PROVIDER_ENTRY_POINT_GROUP" |
| <a id="s-cb34bd75ce56"></a>`owner_path` | "reference/gogurt/packages/core" |
| <a id="s-15c0cf5f6f23"></a>`providers` | items=additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` \| additional keys=`distribution`, `name`, `value` |

## Governing policies

- <a id="pa-8d3f083a8c71"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

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
