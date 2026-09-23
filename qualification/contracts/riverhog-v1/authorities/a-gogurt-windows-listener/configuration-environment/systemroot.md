# SystemRoot

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-gogurt-windows-listener:systemroot:3a533d7d93 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e46514ac5a"></a>

| Field | Value |
|---|---|
| <a id="s-0a3f486fd1"></a>`consumers` | `["a-gogurt-windows-listener"]` |
| <a id="s-884373744b"></a>`default_expressions` | `["'C:\\\\Windows'"]` |
| <a id="s-fff4744bb1"></a>`id` | `"a-gogurt-windows-listener:environment:SystemRoot"` |
| <a id="s-33e9f176ca"></a>`input_shape` | `"environment-string"` |
| <a id="s-92c12c2ce2"></a>`name` | `"SystemRoot"` |
| <a id="s-97c3efd577"></a>`owner` | `"a-gogurt-windows-listener"` |

## Governing policies

- <a id="pa-f8b972ea63"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-gogurt-windows-listener:SystemRoot](../../../evidence/sources/authorities.md#src-304a6a9545) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py::TaskSchedulerUserAdapter.\_identity\_command](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py); [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py::TaskSchedulerUserAdapter.\_state\_command](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-gogurt-windows-listener` | [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py) | `os.environ.get('SystemRoot', 'C:\\Windows')` |
| parser | `a-gogurt-windows-listener` | [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py) | `os.environ.get('SystemRoot', 'C:\\Windows')` |

### Machine authority

- `/external_contract/configuration_environment/4`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1982c79da1b229837a2285afa9f71e2808332e9fd48473eb885c0485be237177 -->

```json
{
  "consumers": [
    "a-gogurt-windows-listener"
  ],
  "default_expressions": [
    "'C:\\\\Windows'"
  ],
  "id": "a-gogurt-windows-listener:environment:SystemRoot",
  "input_shape": "environment-string",
  "name": "SystemRoot",
  "owner": "a-gogurt-windows-listener"
}
```

</details>
