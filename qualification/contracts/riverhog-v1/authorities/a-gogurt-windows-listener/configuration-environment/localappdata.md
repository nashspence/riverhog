# LOCALAPPDATA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-gogurt-windows-listener:localappdata:18e957d761 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b221afcadd"></a>

| Field | Value |
|---|---|
| <a id="s-81eb745f84"></a>`consumers` | `["a-gogurt-windows-listener"]` |
| <a id="s-f4cd2aeccb"></a>`default_expressions` | `["unset"]` |
| <a id="s-96466e0755"></a>`id` | `"a-gogurt-windows-listener:environment:LOCALAPPDATA"` |
| <a id="s-0cdeeaee7c"></a>`input_shape` | `"environment-string"` |
| <a id="s-9bc5608703"></a>`name` | `"LOCALAPPDATA"` |
| <a id="s-79238e712b"></a>`owner` | `"a-gogurt-windows-listener"` |

## Governing policies

- <a id="pa-9d6157318c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-gogurt-windows-listener:LOCALAPPDATA](../../../evidence/sources/authorities.md#src-c37bcd69b9) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py::default\_listener\_paths](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-gogurt-windows-listener` | [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py) | `env.get('LOCALAPPDATA')` |

### Machine authority

- `/external_contract/configuration_environment/2`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b276f93a62e1a0fb7894abc3d9baf37d96aa566a1beb9b3cae5bd0184d3a2b1 -->

```json
{
  "consumers": [
    "a-gogurt-windows-listener"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "a-gogurt-windows-listener:environment:LOCALAPPDATA",
  "input_shape": "environment-string",
  "name": "LOCALAPPDATA",
  "owner": "a-gogurt-windows-listener"
}
```

</details>
