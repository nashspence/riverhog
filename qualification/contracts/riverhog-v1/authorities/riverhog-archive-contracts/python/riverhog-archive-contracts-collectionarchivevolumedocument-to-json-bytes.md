# riverhog_archive_contracts.CollectionArchiveVolumeDocument.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-1beb24743c:8cefa346fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-846af98381"></a>
| Field | Shape |
|---|---|
| <a id="s-b56f4a7b2e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7fb9de2041"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-afbb4b3e8f"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-e687d13542"></a>`name` | "to_json_bytes" |
| <a id="s-83d8d53a47"></a>`owner` | "riverhog_archive_contracts.CollectionArchiveVolumeDocument" |
| <a id="s-88fa58300e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.CollectionArchiveVolumeDocument](riverhog-archive-contracts-collectionarchivevolumedocument.md)

## Governing policies

- <a id="pa-24e4117273"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveVolumeDocument.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc510782dc98daf62b8fd58bdc0d0594c5dfa7d12bf72ed051b8378298128b28 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'builtins.bytes'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "to_json_bytes",
  "owner": "riverhog_archive_contracts.CollectionArchiveVolumeDocument",
  "unit": "member"
}
```
