# a_riverhog_linux_provenance_contract_lib.PLATFORM_FAMILY

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-riverhog-linux-provenance-contract-lib:a-riverhog-linux-provenance-contract-lib-dfe4f6256a:f5b7e94112 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-linux-provenance-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b5dda1dce"></a>
- <a id="s-3779ba72a8"></a>`distribution`: `a-riverhog-linux-provenance-contract-lib`
- <a id="s-af1a638e87"></a>`module`: `a_riverhog_linux_provenance_contract_lib`
- <a id="s-d09e23c9db"></a>`name`: `PLATFORM_FAMILY`
- <a id="s-c8c26f17b0"></a>`unit`: `export`

### Declared structure

- <a id="s-ac79a64012"></a>`kind`: `"constant"`
- <a id="s-114db076a7"></a>`value`: `"linux"`

## Governing policies

- <a id="pa-25942e6ddc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-riverhog-linux-provenance-contract-lib:a_riverhog_linux_provenance_contract_lib](../../../evidence/sources/authorities.md#src-0abeb2023e) — [some-implementations/riverhog/provenance/contracts/linux/src/a\_riverhog\_linux\_provenance\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/riverhog/provenance/contracts/linux/src/a_riverhog_linux_provenance_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_riverhog_linux_provenance_contract_lib.PLATFORM_FAMILY`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cef6126167e63e427bb2f8b14025dfdb29cf68df8acee80c37a61159ad1927a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "linux"
  },
  "distribution": "a-riverhog-linux-provenance-contract-lib",
  "module": "a_riverhog_linux_provenance_contract_lib",
  "name": "PLATFORM_FAMILY",
  "unit": "export"
}
```

</details>
