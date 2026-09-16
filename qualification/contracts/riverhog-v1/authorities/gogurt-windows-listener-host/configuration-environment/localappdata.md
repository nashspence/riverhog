# LOCALAPPDATA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:gogurt-windows-listener-host:localappdata:8a8a05e087 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b221afcadd"></a>

| Field | Value |
|---|---|
| <a id="s-81eb745f84"></a>`consumers` | `["gogurt-windows-listener-host"]` |
| <a id="s-f4cd2aeccb"></a>`default_expressions` | `["unset"]` |
| <a id="s-96466e0755"></a>`id` | `"gogurt-windows-listener-host:environment:LOCALAPPDATA"` |
| <a id="s-0cdeeaee7c"></a>`input_shape` | `"environment-string"` |
| <a id="s-9bc5608703"></a>`name` | `"LOCALAPPDATA"` |
| <a id="s-79238e712b"></a>`owner` | `"gogurt-windows-listener-host"` |

## Governing policies

- <a id="pa-e7d9524de9"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:gogurt-windows-listener-host:LOCALAPPDATA](../../../evidence/sources.md#src-25da23d25d) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `gogurt-windows-listener-host` | `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py` | `env.get('LOCALAPPDATA')` |

### Machine authority

- `/external_contract/configuration_environment/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 909bdcc5e96edfd7d784287a55691614c583ada8268b1e6c7a3ce5c8c9081eaa -->

```json
{
  "consumers": [
    "gogurt-windows-listener-host"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "gogurt-windows-listener-host:environment:LOCALAPPDATA",
  "input_shape": "environment-string",
  "name": "LOCALAPPDATA",
  "owner": "gogurt-windows-listener-host"
}
```

</details>
