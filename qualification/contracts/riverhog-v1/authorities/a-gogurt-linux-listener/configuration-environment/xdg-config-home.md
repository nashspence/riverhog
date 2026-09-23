# XDG_CONFIG_HOME

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-gogurt-linux-listener:xdg-config-home:2c7f740547 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-linux-listener](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-7ba4daea68"></a>

| Field | Value |
|---|---|
| <a id="s-1fb6988e81"></a>`consumers` | `["a-gogurt-linux-listener"]` |
| <a id="s-aea6dc0fbf"></a>`default_expressions` | `["user_home / '.config'"]` |
| <a id="s-580dfdcf0a"></a>`id` | `"a-gogurt-linux-listener:environment:XDG_CONFIG_HOME"` |
| <a id="s-0d6f132719"></a>`input_shape` | `"environment-string"` |
| <a id="s-0df8f11eac"></a>`name` | `"XDG_CONFIG_HOME"` |
| <a id="s-f3d50fff16"></a>`owner` | `"a-gogurt-linux-listener"` |

## Governing policies

- <a id="pa-87726b276e"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-gogurt-linux-listener:XDG_CONFIG_HOME](../../../evidence/sources/authorities.md#src-4288bee9ef) — [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py::\_default\_registration\_file](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-gogurt-linux-listener` | [some-implementations/gogurt/listener-host/linux/src/a\_gogurt\_linux\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/linux/src/a_gogurt_linux_listener/__init__.py) | `env.get('XDG_CONFIG_HOME', user_home / '.config')` |

### Machine authority

- `/external_contract/configuration_environment/0`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca4291a9deab397313fa08cb567e16e67076f0be0ecce22e0e62f17f54bedf50 -->

```json
{
  "consumers": [
    "a-gogurt-linux-listener"
  ],
  "default_expressions": [
    "user_home / '.config'"
  ],
  "id": "a-gogurt-linux-listener:environment:XDG_CONFIG_HOME",
  "input_shape": "environment-string",
  "name": "XDG_CONFIG_HOME",
  "owner": "a-gogurt-linux-listener"
}
```

</details>
