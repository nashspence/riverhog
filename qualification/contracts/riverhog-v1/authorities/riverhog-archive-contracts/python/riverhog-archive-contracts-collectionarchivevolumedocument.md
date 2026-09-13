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
| Field | Shape |
|---|---|
| <a id="s-1ddee6bb39"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-4076ec185d"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-1f5676adac"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-80147fb189"></a>`name` | "CollectionArchiveVolumeDocument" |
| <a id="s-a61316f7a4"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.CollectionArchiveVolumeDocument.to_mapping](riverhog-archive-contracts-collectionarchivevolumedocument-to-mapping.md)
- [riverhog_archive_contracts.CollectionArchiveVolumeDocument.to_json_bytes](riverhog-archive-contracts-collectionarchivevolumedocument-to-json-bytes.md)
- [riverhog_archive_contracts.CollectionArchiveVolumeDocument.from_mapping](riverhog-archive-contracts-collectionarchivevolumedocument-from-mapping.md)
- [riverhog_archive_contracts.CollectionArchiveVolumeDocument.from_json_bytes](riverhog-archive-contracts-collectionarchivevolumedocument-from-json-bytes.md)

## Governing policies

- <a id="pa-f70957a20f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveVolumeDocument`

### Exact owned JSON

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
