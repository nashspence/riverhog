# a_riverhog_linux_provenance_contract_lib.load_schemas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-linux-provenance-contract-lib:a-riverhog-linux-provenance-contract-lib-6a13abc0ab:c65bcdf935 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-linux-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ac74d37827"></a>
- <a id="s-655975001c"></a>`distribution`: `a-riverhog-linux-provenance-contract-lib`
- <a id="s-6eed332016"></a>`module`: `a_riverhog_linux_provenance_contract_lib`
- <a id="s-307b24e2e6"></a>`name`: `load_schemas`
- <a id="s-23de6cb138"></a>`unit`: `export`

### Declared structure

- <a id="s-9f2629698a"></a>`kind`: `"function"`
- <a id="s-4ceab7eb64"></a>`signature`: `"\"() -> 'dict[str, dict[str, Any]]'\""`

## Governing policies

- <a id="pa-c9538457aa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-linux-provenance-contract-lib:a_riverhog_linux_provenance_contract_lib](../../../evidence/sources/authorities.md#src-0abeb2023e) — [some-implementations/riverhog/provenance/contracts/linux/src/a\_riverhog\_linux\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/linux/src/a_riverhog_linux_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_linux_provenance_contract_lib.load_schemas`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cc5b73ba6f99f6ad770bc4546d2ddfb4ac76953ee55c72d75c2a5520a465baa -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"() -> 'dict[str, dict[str, Any]]'\""
  },
  "distribution": "a-riverhog-linux-provenance-contract-lib",
  "module": "a_riverhog_linux_provenance_contract_lib",
  "name": "load_schemas",
  "unit": "export"
}
```

</details>
