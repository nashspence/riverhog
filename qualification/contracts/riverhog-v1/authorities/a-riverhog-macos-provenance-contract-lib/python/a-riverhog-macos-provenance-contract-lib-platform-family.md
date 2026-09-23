# a_riverhog_macos_provenance_contract_lib.PLATFORM_FAMILY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-macos-provenance-contract-lib:a-riverhog-macos-provenance-contract-lib-dd202ca4d5:693a3151b6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-macos-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-431283c85e"></a>
- <a id="s-5ee6829f4f"></a>`distribution`: `a-riverhog-macos-provenance-contract-lib`
- <a id="s-dcd5665c52"></a>`module`: `a_riverhog_macos_provenance_contract_lib`
- <a id="s-b13ddc3392"></a>`name`: `PLATFORM_FAMILY`
- <a id="s-dc1c29e98b"></a>`unit`: `export`

### Declared structure

- <a id="s-9d2d106899"></a>`kind`: `"constant"`
- <a id="s-4438cee507"></a>`value`: `"macos"`

## Governing policies

- <a id="pa-5bfdfff181"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-macos-provenance-contract-lib:a_riverhog_macos_provenance_contract_lib](../../../evidence/sources/authorities.md#src-273ca071fb) — [some-implementations/riverhog/provenance/contracts/macos/src/a\_riverhog\_macos\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/macos/src/a_riverhog_macos_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_macos_provenance_contract_lib.PLATFORM_FAMILY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c3f492c09627cccb6a1aba37522307b918801b80c495f5b0694edcf0fe7596e -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "macos"
  },
  "distribution": "a-riverhog-macos-provenance-contract-lib",
  "module": "a_riverhog_macos_provenance_contract_lib",
  "name": "PLATFORM_FAMILY",
  "unit": "export"
}
```

</details>
