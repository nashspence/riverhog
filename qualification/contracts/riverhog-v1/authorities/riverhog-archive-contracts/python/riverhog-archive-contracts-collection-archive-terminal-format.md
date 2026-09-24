# riverhog_archive_contracts.COLLECTION_ARCHIVE_TERMINAL_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collection-arc-d282246ae7:a461bf9b9a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e573f19c52"></a>
- <a id="s-9592397f9c"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-d51e533904"></a>`module`: `riverhog_archive_contracts`
- <a id="s-60d7bfdf72"></a>`name`: `COLLECTION_ARCHIVE_TERMINAL_FORMAT`
- <a id="s-ee281cf7dc"></a>`unit`: `export`

### Declared structure

- <a id="s-051c067ed7"></a>`kind`: `"constant"`
- <a id="s-372cd31c42"></a>`value`: `"collection-archive-terminal/v1"`

## Governing policies

- <a id="pa-6f505c1d1a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.COLLECTION_ARCHIVE_TERMINAL_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 021e0aa53b0364d67d95b77b80cbb37f4a889565ea0b900941ed7ea01a39b9cd -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-archive-terminal/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "COLLECTION_ARCHIVE_TERMINAL_FORMAT",
  "unit": "export"
}
```

</details>
