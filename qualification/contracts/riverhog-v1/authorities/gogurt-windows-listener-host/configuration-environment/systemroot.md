# SystemRoot

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:gogurt-windows-listener-host:systemroot:83774b4fb3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-windows-listener-host](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-e46514ac5a"></a>

| Field | Value |
|---|---|
| <a id="s-0a3f486fd1"></a>`consumers` | `["gogurt-windows-listener-host"]` |
| <a id="s-884373744b"></a>`default_expressions` | `["'C:\\\\Windows'"]` |
| <a id="s-fff4744bb1"></a>`id` | `"gogurt-windows-listener-host:environment:SystemRoot"` |
| <a id="s-33e9f176ca"></a>`input_shape` | `"environment-string"` |
| <a id="s-92c12c2ce2"></a>`name` | `"SystemRoot"` |
| <a id="s-97c3efd577"></a>`owner` | `"gogurt-windows-listener-host"` |

## Governing policies

- <a id="pa-af989829f0"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:gogurt-windows-listener-host:SystemRoot](../../../evidence/sources.md#src-1770457f4b) — [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py::TaskSchedulerUserAdapter.\_identity\_command](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py); [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py::TaskSchedulerUserAdapter.\_state\_command](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `gogurt-windows-listener-host` | [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py) | `os.environ.get('SystemRoot', 'C:\\Windows')` |
| parser | `gogurt-windows-listener-host` | [reference/gogurt/listener-host/windows/src/gogurt\_windows\_listener\_host/\_\_init\_\_.py](../../../../../../reference/gogurt/listener-host/windows/src/gogurt_windows_listener_host/__init__.py) | `os.environ.get('SystemRoot', 'C:\\Windows')` |

### Machine authority

- `/external_contract/configuration_environment/4`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13852a76170b471e65d4327f0ad346ecc166aa994f69d6212e156d3c9e5dcc3f -->

```json
{
  "consumers": [
    "gogurt-windows-listener-host"
  ],
  "default_expressions": [
    "'C:\\\\Windows'"
  ],
  "id": "gogurt-windows-listener-host:environment:SystemRoot",
  "input_shape": "environment-string",
  "name": "SystemRoot",
  "owner": "gogurt-windows-listener-host"
}
```

</details>
