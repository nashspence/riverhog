# riverhog_archive_contracts.ArchiveVolume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archivevolume:6fb15de457 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b41db5c664"></a>
- <a id="s-2bfa99c48a"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-500af61d7a"></a>`module`: `riverhog_archive_contracts`
- <a id="s-1a587c779c"></a>`name`: `ArchiveVolume`
- <a id="s-b62471df83"></a>`unit`: `export`

### Declared structure

- <a id="s-487c4547e3"></a>`kind`: `"object"`
- <a id="s-5d57f94d9d"></a>`type`: `"types.UnionType"`

## Governing policies

- <a id="pa-da991680d6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ArchiveVolume`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fec1805cbe2632f8a9bc06abc47b1f35c78c87058618dc4d42474bfba5bc4c5 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "types.UnionType"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ArchiveVolume",
  "unit": "export"
}
```

</details>
