# riverhog_archive_contracts.format_archive_sequence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-format-archive-sequence:7f315b391e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db4572e013"></a>
- <a id="s-df3951f771"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-fc1ecf8246"></a>`module`: `riverhog_archive_contracts`
- <a id="s-6b7d9e990e"></a>`name`: `format_archive_sequence`
- <a id="s-2ee5fbaf57"></a>`unit`: `export`

### Declared structure

- <a id="s-1d6e9e5746"></a>`kind`: `"function"`
- <a id="s-51afd1ac98"></a>`signature`: `"\"(value: 'int') -> 'str'\""`

## Governing policies

- <a id="pa-e7625d0465"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.format_archive_sequence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd1a079b3c0c619e3d379b570447b723a18bb9308f85c58eba7835822e0bc348 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'int') -> 'str'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "format_archive_sequence",
  "unit": "export"
}
```

</details>
