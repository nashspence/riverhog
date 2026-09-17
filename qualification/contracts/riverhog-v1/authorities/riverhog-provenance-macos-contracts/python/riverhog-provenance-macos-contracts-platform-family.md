# riverhog_provenance_macos_contracts.PLATFORM_FAMILY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-macos-contracts:riverhog-provenance-macos-contracts-platform-family:e3349b4316 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-macos-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c8182135e"></a>
- <a id="s-239d3a652d"></a>`distribution`: `riverhog-provenance-macos-contracts`
- <a id="s-fbc4de8f2f"></a>`module`: `riverhog_provenance_macos_contracts`
- <a id="s-1cb7b4ec75"></a>`name`: `PLATFORM_FAMILY`
- <a id="s-2c2c402de8"></a>`unit`: `export`

### Declared structure

- <a id="s-414b697a55"></a>`kind`: `"constant"`
- <a id="s-0afa4db4fa"></a>`value`: `"macos"`

## Governing policies

- <a id="pa-7d37b73708"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance-macos-contracts:riverhog_provenance_macos_contracts](../../../evidence/sources/authorities.md#src-75fa891e7c) — [reference/riverhog/provenance/contracts/macos/src/riverhog\_provenance\_macos\_contracts/\_\_init\_\_.py](../../../../../../reference/riverhog/provenance/contracts/macos/src/riverhog_provenance_macos_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance_macos_contracts.PLATFORM_FAMILY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c43cfbdd3f56fbb75108c8a4b87b8c2bba68e353426da7409820e8971cecd108 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "macos"
  },
  "distribution": "riverhog-provenance-macos-contracts",
  "module": "riverhog_provenance_macos_contracts",
  "name": "PLATFORM_FAMILY",
  "unit": "export"
}
```

</details>
