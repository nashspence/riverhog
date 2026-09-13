# riverhog_archive_contracts.ArchiveFileIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-archivefileidentity:2d4c4d15e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f25122136"></a>
| Field | Shape |
|---|---|
| <a id="s-460e3e8927"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-74b79663bb"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-dda825ab78"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-51ec735b50"></a>`name` | "ArchiveFileIdentity" |
| <a id="s-cefbcb2eac"></a>`unit` | "export" |

## Governing policies

- <a id="pa-964b2c507c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.ArchiveFileIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 93ad6a6706a57077da3f594aa360c4ab9e8f1567efb1a9b62c7027db22956602 -->

```json
{
  "contract": {
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
    "signature": "\"(path: 'str', bytes: 'int', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "ArchiveFileIdentity",
  "unit": "export"
}
```
