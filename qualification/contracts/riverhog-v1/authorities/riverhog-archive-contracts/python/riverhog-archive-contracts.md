# riverhog_archive_contracts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts:9bd83ff525 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-archive-contracts` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/4`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:riverhog-archive-contracts` — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `distribution` | "riverhog-archive-contracts" |
| `exports` | object (39 fields) |
| `module` | "riverhog_archive_contracts" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74c44223339f7f7e1d12461497c7e26ef8610a0c1a069a578634464e396f895c -->

```json
{
  "distribution": "riverhog-archive-contracts",
  "exports": {
    "AGE_UPLOAD_STATE_FORMAT": {
      "kind": "constant",
      "value": "age-v1-scrypt-resumable"
    },
    "ARCHIVE_ENCRYPTION_FORMAT": {
      "kind": "constant",
      "value": "age-v1-scrypt"
    },
    "ARCHIVE_PACK_FILES_MAX": {
      "kind": "constant",
      "value": 50000
    },
    "ARCHIVE_ROOT_DOCUMENT_BYTES_MAX": {
      "kind": "constant",
      "value": 65536
    },
    "ARCHIVE_SEQUENCE_BITS": {
      "kind": "constant",
      "value": 256
    },
    "ARCHIVE_SEQUENCE_HEX_WIDTH": {
      "kind": "constant",
      "value": 64
    },
    "ARCHIVE_VOLUME_DOCUMENT_BYTES_MAX": {
      "kind": "constant",
      "value": 1048576
    },
    "ARCHIVE_VOLUME_PARTS_MAX": {
      "kind": "constant",
      "value": 1024
    },
    "AgeUploadState": {
      "fields": [
        {
          "default": "required",
          "name": "header_b64",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "payload_nonce_b64",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "plaintext_size",
          "type": "'int'"
        },
        {
          "default": "'age-v1-scrypt-resumable'",
          "name": "format",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object', *, plaintext_bytes: 'int') -> 'AgeUploadState'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(header_b64: 'str', payload_nonce_b64: 'str', plaintext_size: 'int', format: 'str' = 'age-v1-scrypt-resumable') -> None"
    },
    "ArchiveFileIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(path: 'str', bytes: 'int', sha256: 'str') -> None"
    },
    "ArchiveManifestError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "ArchiveProvenanceIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "identity",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "root",
          "type": "'ProvenanceRootIdentity'"
        }
      ],
      "kind": "class",
      "members": {
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'ArchiveProvenanceIdentity'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(identity: 'str', root: 'ProvenanceRootIdentity') -> None"
    },
    "ArchiveRootCiphertextIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "stored_bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "stored_sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(path: 'str', stored_bytes: 'int', stored_sha256: 'str') -> None"
    },
    "ArchiveVolume": {
      "kind": "object",
      "type": "types.UnionType"
    },
    "COLLECTION_ARCHIVE_MANIFEST_SCHEMA": {
      "kind": "constant",
      "value": "collection-archive-manifest/v1"
    },
    "COLLECTION_ARCHIVE_TERMINAL_SCHEMA": {
      "kind": "constant",
      "value": "collection-archive-terminal/v1"
    },
    "COLLECTION_ARCHIVE_VOLUME_SCHEMA": {
      "kind": "constant",
      "value": "collection-archive-volume/v1"
    },
    "CollectionArchiveManifest": {
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
          "name": "schema",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "bytes": {
          "kind": "property",
          "signature": "(self) -> 'int'"
        },
        "files": {
          "kind": "property",
          "signature": "(self) -> 'int'"
        },
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, content: 'bytes | str') -> 'CollectionArchiveManifest'"
        },
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'CollectionArchiveManifest'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'builtins.bytes'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        },
        "tree_sha256": {
          "kind": "property",
          "signature": "(self) -> 'str'"
        }
      },
      "signature": "(archive_generation: 'str', tree: 'CollectionTreeIdentity', ordered_volume_sha256: 'str', provenance: 'ArchiveProvenanceIdentity | None' = None, schema: 'str' = 'collection-archive-manifest/v1') -> None"
    },
    "CollectionArchiveTerminalDocument": {
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
          "name": "sequence",
          "type": "'int'"
        },
        {
          "default": "'terminal'",
          "name": "kind",
          "type": "\"Literal['terminal']\""
        },
        {
          "default": "'collection-archive-terminal/v1'",
          "name": "schema",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, content: 'bytes | str') -> 'CollectionArchiveTerminalDocument'"
        },
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'CollectionArchiveTerminalDocument'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'builtins.bytes'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(archive_generation: 'str', archive_tree_sha256: 'str', sequence: 'int', kind: \"Literal['terminal']\" = 'terminal', schema: 'str' = 'collection-archive-terminal/v1') -> None"
    },
    "CollectionArchiveVolumeDocument": {
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
      "members": {
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, content: 'bytes | str') -> 'CollectionArchiveVolumeDocument'"
        },
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'CollectionArchiveVolumeDocument'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'builtins.bytes'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(archive_generation: 'str', archive_tree_sha256: 'str', volume: 'ArchiveVolume', schema: 'str' = 'collection-archive-volume/v1') -> None"
    },
    "CollectionEncryptionBinding": {
      "fields": [
        {
          "default": "required",
          "name": "format",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "passphrase_id",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "signature": "(format: 'str', passphrase_id: 'str') -> None"
    },
    "CollectionTreeIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "files",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'CollectionTreeIdentity'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(files: 'int', bytes: 'int', sha256: 'str') -> None"
    },
    "PACK_INDEX_SCHEMA": {
      "kind": "constant",
      "value": "riverhog-pack-index/v1"
    },
    "PART_DIGEST_FORMAT": {
      "kind": "constant",
      "value": "sha256"
    },
    "PackArchiveVolume": {
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
          "name": "files",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "source_bytes",
          "type": "'int'"
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
          "name": "index_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "plan_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "parts",
          "type": "'tuple[StoredPartIdentity, ...]'"
        },
        {
          "default": "'pack'",
          "name": "kind",
          "type": "\"Literal['pack']\""
        }
      ],
      "kind": "class",
      "members": {
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(id: 'str', sequence: 'int', path: 'str', files: 'int', source_bytes: 'int', plaintext_bytes: 'int', age_state: 'AgeUploadState', index_sha256: 'str', plan_sha256: 'str', parts: 'tuple[StoredPartIdentity, ...]', kind: \"Literal['pack']\" = 'pack') -> None"
    },
    "ProvenanceRootIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "kind",
          "type": "\"Literal['provenance-root']\""
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
          "name": "sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "stored_bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "stored_sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object') -> 'ProvenanceRootIdentity'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(id: 'str', kind: \"Literal['provenance-root']\", path: 'str', plaintext_bytes: 'int', sha256: 'str', stored_bytes: 'int', stored_sha256: 'str') -> None"
    },
    "RECOVERY_DESCRIPTOR_PATH": {
      "kind": "constant",
      "value": "recovery.json"
    },
    "RECOVERY_DESCRIPTOR_SCHEMA": {
      "kind": "constant",
      "value": "riverhog-recovery-descriptor/v1"
    },
    "RecoveryDescriptor": {
      "fields": [
        {
          "default": "required",
          "name": "encryption",
          "type": "'CollectionEncryptionBinding'"
        },
        {
          "default": "required",
          "name": "root",
          "type": "'ArchiveRootCiphertextIdentity'"
        },
        {
          "default": "'riverhog-recovery-descriptor/v1'",
          "name": "schema",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_json_bytes": {
          "kind": "classmethod",
          "signature": "(cls, content: 'bytes | str') -> 'RecoveryDescriptor'"
        },
        "to_json_bytes": {
          "kind": "method",
          "signature": "(self) -> 'bytes'"
        }
      },
      "signature": "(encryption: 'CollectionEncryptionBinding', root: 'ArchiveRootCiphertextIdentity', schema: 'str' = 'riverhog-recovery-descriptor/v1') -> None"
    },
    "RecoveryDescriptorError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "SELECTIVE_READ_FORMAT": {
      "kind": "constant",
      "value": "age-chunk-range/v1"
    },
    "SegmentArchiveVolume": {
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
      "members": {
        "file_offset": {
          "kind": "property",
          "signature": "(self) -> 'int'"
        },
        "source_file": {
          "kind": "property",
          "signature": "(self) -> 'ArchiveFileIdentity'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(id: 'str', sequence: 'int', path: 'str', plaintext_bytes: 'int', age_state: 'AgeUploadState', file: 'SegmentFilePlacement', parts: 'tuple[StoredPartIdentity, ...]', kind: \"Literal['segment']\" = 'segment') -> None"
    },
    "SegmentFilePlacement": {
      "fields": [
        {
          "default": "required",
          "name": "path",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "offset",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "file_bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object', *, plaintext_bytes: 'int') -> 'SegmentFilePlacement'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(path: 'str', offset: 'int', bytes: 'int', file_bytes: 'int', sha256: 'str') -> None"
    },
    "StoredPartIdentity": {
      "fields": [
        {
          "default": "required",
          "name": "number",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "plaintext_start",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "plaintext_bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "plaintext_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "stored_bytes",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "stored_sha256",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "from_mapping": {
          "kind": "classmethod",
          "signature": "(cls, value: 'object', *, expected_number: 'int', expected_start: 'int') -> 'StoredPartIdentity'"
        },
        "to_mapping": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, object]'"
        }
      },
      "signature": "(number: 'int', plaintext_start: 'int', plaintext_bytes: 'int', plaintext_sha256: 'str', stored_bytes: 'int', stored_sha256: 'str') -> None"
    },
    "format_archive_sequence": {
      "kind": "function",
      "signature": "(value: 'int') -> 'str'"
    },
    "normalize_passphrase_id": {
      "kind": "function",
      "signature": "(value: 'str') -> 'str'"
    },
    "ordered_archive_volume_commitment": {
      "kind": "function",
      "signature": "(documents: 'Iterable[ArchiveSequenceDocument]') -> 'str'"
    },
    "parse_archive_sequence": {
      "kind": "function",
      "signature": "(value: 'object', label: 'str' = 'archive sequence') -> 'int'"
    },
    "update_archive_sequence_commitment": {
      "kind": "function",
      "signature": "(digest: 'Any', document: 'ArchiveSequenceDocument') -> 'None'"
    }
  },
  "module": "riverhog_archive_contracts"
}
```
