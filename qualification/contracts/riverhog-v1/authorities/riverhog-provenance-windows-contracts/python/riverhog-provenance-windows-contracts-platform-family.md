# riverhog_provenance_windows_contracts.PLATFORM_FAMILY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-windows-contracts:riverhog-provenance-windows-contracts-pla-40509c2381:03992affb9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-windows-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f87741bf4"></a>
- <a id="s-0bcc5510ab"></a>`distribution`: `riverhog-provenance-windows-contracts`
- <a id="s-cab10690d5"></a>`module`: `riverhog_provenance_windows_contracts`
- <a id="s-bb179fabc2"></a>`name`: `PLATFORM_FAMILY`
- <a id="s-bb07ce0046"></a>`unit`: `export`

### Declared structure

- <a id="s-f5a5c4c793"></a>`kind`: `"constant"`
- <a id="s-bece528387"></a>`value`: `"windows"`

## Governing policies

- <a id="pa-c8924a0434"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance-windows-contracts:riverhog_provenance_windows_contracts](../../../evidence/sources/authorities.md#src-0d5e61922a) — [reference/riverhog/provenance/contracts/windows/src/riverhog\_provenance\_windows\_contracts/\_\_init\_\_.py](../../../../../../reference/riverhog/provenance/contracts/windows/src/riverhog_provenance_windows_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance_windows_contracts.PLATFORM_FAMILY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0f4fc6ceab5ce9f94f0e42913d7b100bae43678dfe69d5c645b6b013776cd21 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "windows"
  },
  "distribution": "riverhog-provenance-windows-contracts",
  "module": "riverhog_provenance_windows_contracts",
  "name": "PLATFORM_FAMILY",
  "unit": "export"
}
```

</details>
