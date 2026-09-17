# riverhog_archive_contracts.CollectionArchiveVolumeDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-828cdd84f1:2d856de96b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0692109b01"></a>
- <a id="s-4076ec185d"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-1f5676adac"></a>`module`: `riverhog_archive_contracts`
- <a id="s-80147fb189"></a>`name`: `CollectionArchiveVolumeDocument`
- <a id="s-a61316f7a4"></a>`unit`: `export`

### Declared structure

- <a id="s-41d8cb5c9a"></a>`kind`: `"class"`
- <a id="s-bb64e5154a"></a>`signature`: `"\"(archive_generation: 'str', archive_tree_sha256: 'str', volume: 'ArchiveVolume', schema: 'str' = 'collection-archive-volume/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-0b6dfe1efc"></a>`archive_generation` | `'str'` | `required` |
| <a id="s-ccd494cd45"></a>`archive_tree_sha256` | `'str'` | `required` |
| <a id="s-aa6c76dea9"></a>`volume` | `'ArchiveVolume'` | `required` |
| <a id="s-fe3b05b275"></a>`schema` | `'str'` | `'collection-archive-volume/v1'` |

## Maintained corroboration

### Related interface records

- [to_mapping](riverhog-archive-contracts-collectionarchivevolumedocument-to-mapping.md)
- [to_json_bytes](riverhog-archive-contracts-collectionarchivevolumedocument-to-json-bytes.md)
- [from_mapping](riverhog-archive-contracts-collectionarchivevolumedocument-from-mapping.md)
- [from_json_bytes](riverhog-archive-contracts-collectionarchivevolumedocument-from-json-bytes.md)

## Governing policies

- <a id="pa-f70957a20f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveVolumeDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4debd0ad8451cb5a5763d212647a27ecbf6c2d43325508317bd90b09fe788630 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "archive_generation",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "archive_tree_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "volume",
        "type": "'ArchiveVolume'"
      },
      {
        "default": "'collection-archive-volume/v1'",
        "name": "schema",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(archive_generation: 'str', archive_tree_sha256: 'str', volume: 'ArchiveVolume', schema: 'str' = 'collection-archive-volume/v1') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "CollectionArchiveVolumeDocument",
  "unit": "export"
}
```

</details>
