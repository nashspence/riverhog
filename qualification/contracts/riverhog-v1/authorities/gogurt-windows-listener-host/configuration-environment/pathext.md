# PATHEXT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:gogurt-windows-listener-host:pathext:90843c9ab8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Configuration Environment](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bf86dd537b"></a>
| Field | Shape |
|---|---|
| <a id="s-98d6331adc"></a>`consumers` | ["gogurt-windows-listener-host"] |
| <a id="s-08725fd7d0"></a>`default_expressions` | ["'.COM;.EXE;.BAT;.CMD'"] |
| <a id="s-7ab9da7b1b"></a>`id` | "gogurt-windows-listener-host:environment:PATHEXT" |
| <a id="s-7a3e545630"></a>`input_shape` | "environment-string" |
| <a id="s-ffbf567bef"></a>`name` | "PATHEXT" |
| <a id="s-8d1db45f77"></a>`owner` | "gogurt-windows-listener-host" |

## Governing policies

- <a id="pa-e2786670eb"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:gogurt-windows-listener-host:PATHEXT](../../../evidence/sources.md#src-3463aeca7f) — `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `gogurt-windows-listener-host` | `reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py` | `os.environ.get('PATHEXT', '.COM;.EXE;.BAT;.CMD')` |

### Machine authority

- `/external_contract/configuration_environment/3`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20cba2b34f487d85e22ae3fec3f167d316359438c10a0d33c317c23d650a3ed9 -->

```json
{
  "consumers": [
    "gogurt-windows-listener-host"
  ],
  "default_expressions": [
    "'.COM;.EXE;.BAT;.CMD'"
  ],
  "id": "gogurt-windows-listener-host:environment:PATHEXT",
  "input_shape": "environment-string",
  "name": "PATHEXT",
  "owner": "gogurt-windows-listener-host"
}
```
