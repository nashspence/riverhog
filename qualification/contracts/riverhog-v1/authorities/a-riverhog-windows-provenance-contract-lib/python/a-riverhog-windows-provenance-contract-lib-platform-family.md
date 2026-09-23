# a_riverhog_windows_provenance_contract_lib.PLATFORM_FAMILY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-windows-provenance-contract-lib:a-riverhog-windows-provenance-contract-li-0e530b4d53:4e0a0fb832 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-windows-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-49480ba642"></a>
- <a id="s-0eb7f51be9"></a>`distribution`: `a-riverhog-windows-provenance-contract-lib`
- <a id="s-e5328ff630"></a>`module`: `a_riverhog_windows_provenance_contract_lib`
- <a id="s-0f06cbe265"></a>`name`: `PLATFORM_FAMILY`
- <a id="s-b6a896231e"></a>`unit`: `export`

### Declared structure

- <a id="s-9350fd4821"></a>`kind`: `"constant"`
- <a id="s-34113a007b"></a>`value`: `"windows"`

## Governing policies

- <a id="pa-e9ccef4479"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-windows-provenance-contract-lib:a_riverhog_windows_provenance_contract_lib](../../../evidence/sources/authorities.md#src-85ad7caa5f) — [some-implementations/riverhog/provenance/contracts/windows/src/a\_riverhog\_windows\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/windows/src/a_riverhog_windows_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_windows_provenance_contract_lib.PLATFORM_FAMILY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5433fda6882ad5b6081d5d62972f9018998bfbb203efea2709472ee952d6242 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "windows"
  },
  "distribution": "a-riverhog-windows-provenance-contract-lib",
  "module": "a_riverhog_windows_provenance_contract_lib",
  "name": "PLATFORM_FAMILY",
  "unit": "export"
}
```

</details>
