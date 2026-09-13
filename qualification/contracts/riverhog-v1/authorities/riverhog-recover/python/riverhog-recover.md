# riverhog_recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover:7579941c68 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3a44c47232"></a>
| Field | Shape |
|---|---|
| <a id="s-878ffbf1ed"></a>`candidate_id` | "python:riverhog-recover:riverhog_recover" |
| <a id="s-d0a1513400"></a>`distribution` | "riverhog-recover" |
| <a id="s-b03316bfeb"></a>`exports` | additional keys=`RecoveredCollectionTags`, `RecoveryError`, `RecoverySummary`, `recover_archive`, `recover_collection_description`, `recover_collection_tags` |
| <a id="s-88a7726aec"></a>`module` | "riverhog_recover" |

## Governing policies

- <a id="pa-5b912a4078"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources.md#src-dbfe6c5e2e) — `reference/riverhog/recovery/src/riverhog_recover/__init__.py`

### Machine authority

- `/external_contract/python/24`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e1a9f885160a6714e59ba708897c5c7363f6fb27abe7b0ed085e9f12d098796 -->

```json
{
  "candidate_id": "python:riverhog-recover:riverhog_recover",
  "distribution": "riverhog-recover",
  "exports": {
    "RecoveredCollectionTags": {
      "kind": "class",
      "members": {
        "iter_tags": {
          "kind": "method",
          "signature": "\"(self) -> 'Iterator[str]'\""
        }
      },
      "signature": "\"(*, archive: 'Path', passphrase: 'str', age_command: 'str', head: 'CollectionTagHeadDocument') -> 'None'\""
    },
    "RecoveryError": {
      "kind": "class",
      "signature": "unavailable"
    },
    "RecoverySummary": {
      "fields": [
        {
          "default": "required",
          "name": "output",
          "type": "'Path'"
        },
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
          "name": "volumes",
          "type": "'int'"
        },
        {
          "default": "'omitted'",
          "name": "provenance_mode",
          "type": "'str'"
        },
        {
          "default": "0",
          "name": "provenance_journals",
          "type": "'int'"
        }
      ],
      "kind": "class",
      "signature": "\"(output: 'Path', files: 'int', bytes: 'int', volumes: 'int', provenance_mode: 'str' = 'omitted', provenance_journals: 'int' = 0) -> None\""
    },
    "recover_archive": {
      "kind": "function",
      "signature": "\"(archive_dir: 'Path', output_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoverySummary'\""
    },
    "recover_collection_description": {
      "kind": "function",
      "signature": "\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'CollectionDescriptionDocument | None'\""
    },
    "recover_collection_tags": {
      "kind": "function",
      "signature": "\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'RecoveredCollectionTags'\""
    }
  },
  "module": "riverhog_recover"
}
```
