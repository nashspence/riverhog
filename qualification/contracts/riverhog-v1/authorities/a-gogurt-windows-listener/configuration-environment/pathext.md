# PATHEXT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-gogurt-windows-listener:pathext:ac0ccbd1f5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-gogurt-windows-listener](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bf86dd537b"></a>

| Field | Value |
|---|---|
| <a id="s-98d6331adc"></a>`consumers` | `["a-gogurt-windows-listener"]` |
| <a id="s-08725fd7d0"></a>`default_expressions` | `["'.COM;.EXE;.BAT;.CMD'"]` |
| <a id="s-7ab9da7b1b"></a>`id` | `"a-gogurt-windows-listener:environment:PATHEXT"` |
| <a id="s-7a3e545630"></a>`input_shape` | `"environment-string"` |
| <a id="s-ffbf567bef"></a>`name` | `"PATHEXT"` |
| <a id="s-8d1db45f77"></a>`owner` | `"a-gogurt-windows-listener"` |

## Governing policies

- <a id="pa-fd8d40cfd5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-gogurt-windows-listener:PATHEXT](../../../evidence/sources/authorities.md#src-242feed8ee) — [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py::resolve\_listener\_executable](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-gogurt-windows-listener` | [some-implementations/gogurt/listener-host/windows/src/a\_gogurt\_windows\_listener/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/listener-host/windows/src/a_gogurt_windows_listener/__init__.py) | `os.environ.get('PATHEXT', '.COM;.EXE;.BAT;.CMD')` |

### Machine authority

- `/external_contract/configuration_environment/3`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed1e7c4eb46ce722fe39e52f670715b58c3eadbc372c0753051a8b99de790c44 -->

```json
{
  "consumers": [
    "a-gogurt-windows-listener"
  ],
  "default_expressions": [
    "'.COM;.EXE;.BAT;.CMD'"
  ],
  "id": "a-gogurt-windows-listener:environment:PATHEXT",
  "input_shape": "environment-string",
  "name": "PATHEXT",
  "owner": "a-gogurt-windows-listener"
}
```

</details>
