# XDG_STATE_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-gogurt-linux-listener:xdg-state-home:68d331a504 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0d1f6f2eed"></a>

| Field | Value |
|---|---|
| <a id="s-1351b3e99a"></a>`consumers` | `["a-gogurt-linux-listener"]` |
| <a id="s-6441796b18"></a>`default_expressions` | `["user_home / '.local' / 'state'"]` |
| <a id="s-8efd3e0ae2"></a>`id` | `"a-gogurt-linux-listener:environment:XDG_STATE_HOME"` |
| <a id="s-aa9de88a18"></a>`input_shape` | `"environment-string"` |
| <a id="s-4e078e6d98"></a>`name` | `"XDG_STATE_HOME"` |
| <a id="s-5f4b8348df"></a>`owner` | `"a-gogurt-linux-listener"` |

## Governing policies

- <a id="pa-daf284f108"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-gogurt-linux-listener:XDG_STATE_HOME](../../../evidence/sources/authorities.md#src-3fec3cc27d) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py::default\_listener\_paths](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-gogurt-linux-listener` | [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py) | `env.get('XDG_STATE_HOME', user_home / '.local' / 'state')` |

### Machine authority

- `/external_contract/configuration_environment/1`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3839d70360b915a64eebc72711b79cac587f72e42e7d83a4bc3119a6f795f875 -->

```json
{
  "consumers": [
    "a-gogurt-linux-listener"
  ],
  "default_expressions": [
    "user_home / '.local' / 'state'"
  ],
  "id": "a-gogurt-linux-listener:environment:XDG_STATE_HOME",
  "input_shape": "environment-string",
  "name": "XDG_STATE_HOME",
  "owner": "a-gogurt-linux-listener"
}
```

</details>
