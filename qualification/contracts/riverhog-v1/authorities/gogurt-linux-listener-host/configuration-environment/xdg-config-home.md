# XDG_CONFIG_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:gogurt-linux-listener-host:xdg-config-home:21b7332408 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-linux-listener-host](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7ba4daea68"></a>

| Field | Value |
|---|---|
| <a id="s-1fb6988e81"></a>`consumers` | `["gogurt-linux-listener-host"]` |
| <a id="s-aea6dc0fbf"></a>`default_expressions` | `["user_home / '.config'"]` |
| <a id="s-580dfdcf0a"></a>`id` | `"gogurt-linux-listener-host:environment:XDG_CONFIG_HOME"` |
| <a id="s-0d6f132719"></a>`input_shape` | `"environment-string"` |
| <a id="s-0df8f11eac"></a>`name` | `"XDG_CONFIG_HOME"` |
| <a id="s-f3d50fff16"></a>`owner` | `"gogurt-linux-listener-host"` |

## Governing policies

- <a id="pa-1f4679b6ad"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:gogurt-linux-listener-host:XDG_CONFIG_HOME](../../../evidence/sources/authorities.md#src-82ea2d68db) — [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py::\_default\_registration\_file](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `gogurt-linux-listener-host` | [reference/gogurt/listener-host/linux/src/gogurt\_linux\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/linux/src/gogurt_linux_listener_host/__init__.py) | `env.get('XDG_CONFIG_HOME', user_home / '.config')` |

### Machine authority

- `/external_contract/configuration_environment/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d279ee6edd991754d852be21141defe6c16080bf4f4de86ddd87d46413ff964 -->

```json
{
  "consumers": [
    "gogurt-linux-listener-host"
  ],
  "default_expressions": [
    "user_home / '.config'"
  ],
  "id": "gogurt-linux-listener-host:environment:XDG_CONFIG_HOME",
  "input_shape": "environment-string",
  "name": "XDG_CONFIG_HOME",
  "owner": "gogurt-linux-listener-host"
}
```

</details>
