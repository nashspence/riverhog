# riverhog_archive_contracts.CollectionArchiveManifest.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-77cafe63fa:fbd8b6a3b6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-65030c6f79"></a>
- <a id="s-345e849532"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-e1a4ca49bc"></a>`module`: `riverhog_archive_contracts`
- <a id="s-f91ddbf4ab"></a>`name`: `from_mapping`
- <a id="s-f459b3a140"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveManifest`
- <a id="s-18a65b200e"></a>`unit`: `member`

### Declared structure

- <a id="s-1964f6cb81"></a>`kind`: `"classmethod"`
- <a id="s-d6e33a740d"></a>`signature`: `"\"(cls, value: 'object') -> 'CollectionArchiveManifest'\""`

## Maintained corroboration

### Related interface records

- [CollectionArchiveManifest](riverhog-archive-contracts-collectionarchivemanifest.md)

## Governing policies

- <a id="pa-931cf2b250"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveManifest.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9c70c7774a6773547047e3dad2d0caa8bb73acfe6a6e03b95cb8891629e1670c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'object') -> 'CollectionArchiveManifest'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_mapping",
  "owner": "riverhog_archive_contracts.CollectionArchiveManifest",
  "unit": "member"
}
```

</details>
