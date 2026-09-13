# riverhog_archive_contracts.SegmentArchiveVolume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-segmentarchivevolume:5fa25ec9ea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-09631df0ca"></a>
| Field | Shape |
|---|---|
| <a id="s-a018ef82c6"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-566710be10"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-212455ca34"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-0d128b01e6"></a>`name` | "SegmentArchiveVolume" |
| <a id="s-ff00ada492"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.SegmentArchiveVolume.file_offset](riverhog-archive-contracts-segmentarchivevolume-file-offset.md)
- [riverhog_archive_contracts.SegmentArchiveVolume.to_mapping](riverhog-archive-contracts-segmentarchivevolume-to-mapping.md)
- [riverhog_archive_contracts.SegmentArchiveVolume.source_file](riverhog-archive-contracts-segmentarchivevolume-source-file.md)

## Governing policies

- <a id="pa-093abd38f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.SegmentArchiveVolume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1bcbf9c530f778561dcea245a06549356cd10103172eccf39beda1665145a6e1 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "sequence",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "plaintext_bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "age_state",
        "type": "'AgeUploadState'"
      },
      {
        "default": "required",
        "name": "file",
        "type": "'SegmentFilePlacement'"
      },
      {
        "default": "required",
        "name": "parts",
        "type": "'tuple[StoredPartIdentity, ...]'"
      },
      {
        "default": "'segment'",
        "name": "kind",
        "type": "\"Literal['segment']\""
      }
    ],
    "kind": "class",
    "signature": "'(id: \\'str\\', sequence: \\'int\\', path: \\'str\\', plaintext_bytes: \\'int\\', age_state: \\'AgeUploadState\\', file: \\'SegmentFilePlacement\\', parts: \\'tuple[StoredPartIdentity, ...]\\', kind: \"Literal[\\'segment\\']\" = \\'segment\\') -> None'"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "SegmentArchiveVolume",
  "unit": "export"
}
```
