# XDG_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:gogurt-linux-listener-host:xdg-state-home:43ea6a678f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0d1f6f2eed"></a>

| Field | Value |
|---|---|
| <a id="s-1351b3e99a"></a>`consumers` | `["gogurt-linux-listener-host"]` |
| <a id="s-6441796b18"></a>`default_expressions` | `["user_home / '.local' / 'state'"]` |
| <a id="s-8efd3e0ae2"></a>`id` | `"gogurt-linux-listener-host:environment:XDG_STATE_HOME"` |
| <a id="s-aa9de88a18"></a>`input_shape` | `"environment-string"` |
| <a id="s-4e078e6d98"></a>`name` | `"XDG_STATE_HOME"` |
| <a id="s-5f4b8348df"></a>`owner` | `"gogurt-linux-listener-host"` |

## Governing policies

- <a id="pa-a0aee3b2ba"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:gogurt-linux-listener-host:XDG_STATE_HOME](../../../evidence/sources.md#src-7517c8a6b8) — `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Configuration authority and bindings

The owning implementation defines the setting. The parser expression records each independently discovered consumer binding and effective default exercised by qualification.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `gogurt-linux-listener-host` | `reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py` | `env.get('XDG_STATE_HOME', user_home / '.local' / 'state')` |

### Machine authority

- `/external_contract/configuration_environment/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc2f8139149b1fe7a901513efdcaa7bee64d3a16406dc63503c9a130323490cd -->

```json
{
  "consumers": [
    "gogurt-linux-listener-host"
  ],
  "default_expressions": [
    "user_home / '.local' / 'state'"
  ],
  "id": "gogurt-linux-listener-host:environment:XDG_STATE_HOME",
  "input_shape": "environment-string",
  "name": "XDG_STATE_HOME",
  "owner": "gogurt-linux-listener-host"
}
```

</details>
