# riverhog_archive_contracts.CollectionArchiveManifest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarchivemanifest:8ce7da2e2d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3b75d56a38"></a>
- <a id="s-b0bfd3c955"></a>`distribution`: `riverhog-archive-contracts`
- <a id="s-c48bdb3045"></a>`module`: `riverhog_archive_contracts`
- <a id="s-65fb308e1a"></a>`name`: `CollectionArchiveManifest`
- <a id="s-ae01f9a25a"></a>`unit`: `export`

### Declared structure

- <a id="s-d55d11bb87"></a>`kind`: `"class"`
- <a id="s-acec45b370"></a>`signature`: `"\"(archive_generation: 'str', tree: 'CollectionTreeIdentity', ordered_volume_sha256: 'str', provenance: 'ArchiveProvenanceIdentity \| None' = None, format: 'str' = 'collection-archive-manifest/v1') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-e534c968be"></a>`archive_generation` | `'str'` | `required` |
| <a id="s-ae4ee3a651"></a>`tree` | `'CollectionTreeIdentity'` | `required` |
| <a id="s-9bcfc1fed4"></a>`ordered_volume_sha256` | `'str'` | `required` |
| <a id="s-04b654329f"></a>`provenance` | `'ArchiveProvenanceIdentity \| None'` | `None` |
| <a id="s-e1caa74619"></a>`format` | `'str'` | `'collection-archive-manifest/v1'` |

## Maintained corroboration

### Related interface records

- [from_json_bytes](riverhog-archive-contracts-collectionarchivemanifest-from-json-bytes.md)
- [bytes](riverhog-archive-contracts-collectionarchivemanifest-bytes.md)
- [to_mapping](riverhog-archive-contracts-collectionarchivemanifest-to-mapping.md)
- [from_mapping](riverhog-archive-contracts-collectionarchivemanifest-from-mapping.md)
- [files](riverhog-archive-contracts-collectionarchivemanifest-files.md)
- [tree_sha256](riverhog-archive-contracts-collectionarchivemanifest-tree-sha256.md)
- [to_json_bytes](riverhog-archive-contracts-collectionarchivemanifest-to-json-bytes.md)

## Governing policies

- <a id="pa-474fbfaa42"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources/authorities.md#src-4557222ddc) — [packages/riverhog-archive-contracts/src/riverhog\_archive\_contracts/\_\_init\_\_.py](../../../../../../packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveManifest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df1e9561d51d21febdb8582abf78dea8a4b49ee7eaaeb05fa35a612da864bf53 -->

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
        "name": "tree",
        "type": "'CollectionTreeIdentity'"
      },
      {
        "default": "required",
        "name": "ordered_volume_sha256",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "provenance",
        "type": "'ArchiveProvenanceIdentity | None'"
      },
      {
        "default": "'collection-archive-manifest/v1'",
        "name": "format",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(archive_generation: 'str', tree: 'CollectionTreeIdentity', ordered_volume_sha256: 'str', provenance: 'ArchiveProvenanceIdentity | None' = None, format: 'str' = 'collection-archive-manifest/v1') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "CollectionArchiveManifest",
  "unit": "export"
}
```

</details>
