# riverhog_archive_contracts.PackArchiveVolume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-packarchivevolume:3090a7249b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ba4643c35"></a>
| Field | Shape |
|---|---|
| <a id="s-637db50c86"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-91e95c1a69"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-2237439c7c"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-b581a0f43d"></a>`name` | "PackArchiveVolume" |
| <a id="s-05777ad172"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.PackArchiveVolume.to_mapping](riverhog-archive-contracts-packarchivevolume-to-mapping.md)

## Governing policies

- <a id="pa-728bfab5e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.PackArchiveVolume`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f17fa09699bf2484606e5654d8ed2948c1b223c8b7c1fc7d14faf1bc7d1aaf97 -->

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
    "signature": "'(id: \\'str\\', sequence: \\'int\\', path: \\'str\\', files: \\'int\\', source_bytes: \\'int\\', plaintext_bytes: \\'int\\', age_state: \\'AgeUploadState\\', index_sha256: \\'str\\', plan_sha256: \\'str\\', parts: \\'tuple[StoredPartIdentity, ...]\\', kind: \"Literal[\\'pack\\']\" = \\'pack\\') -> None'"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "PackArchiveVolume",
  "unit": "export"
}
```
