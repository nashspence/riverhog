# riverhog_archive_contracts.CollectionArchiveVolumeDocument.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-f046746a43:7affea199f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7830d70a30"></a>
- <a id="s-5f736d31b7"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-e860533eed"></a>`module`: `riverhog_archive_contracts`
- <a id="s-28c95a9f03"></a>`name`: `from_json_bytes`
- <a id="s-3fbd5112ab"></a>`owner`: `riverhog_archive_contracts.CollectionArchiveVolumeDocument`
- <a id="s-7eed8e50dd"></a>`unit`: `member`

### Declared structure

- <a id="s-ca846ea613"></a>`kind`: `"classmethod"`
- <a id="s-b0ed05f545"></a>`signature`: `"\"(cls, content: 'bytes \| str') -> 'CollectionArchiveVolumeDocument'\""`

## Maintained corroboration

### Related interface records

- [CollectionArchiveVolumeDocument](riverhog-archive-contracts-collectionarchivevolumedocument.md)

## Governing policies

- <a id="pa-fbc265cde0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveVolumeDocument.from_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5cd824f039b97dce297c6421aef44facdc2781b0491897ac35e5499d22083dbb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes | str') -> 'CollectionArchiveVolumeDocument'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_json_bytes",
  "owner": "riverhog_archive_contracts.CollectionArchiveVolumeDocument",
  "unit": "member"
}
```

</details>
