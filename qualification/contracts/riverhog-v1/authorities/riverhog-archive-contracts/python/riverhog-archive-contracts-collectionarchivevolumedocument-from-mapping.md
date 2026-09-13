# riverhog_archive_contracts.CollectionArchiveVolumeDocument.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collectionarch-3a84b02341:063e3fb482 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-36d518900d"></a>
| Field | Shape |
|---|---|
| <a id="s-f48db2ca14"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f1d4e9eafc"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-3b220ff2a1"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-d1c966d9eb"></a>`name` | "from_mapping" |
| <a id="s-a45258b3ad"></a>`owner` | "riverhog_archive_contracts.CollectionArchiveVolumeDocument" |
| <a id="s-8c0de6a8fc"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_archive_contracts.CollectionArchiveVolumeDocument](riverhog-archive-contracts-collectionarchivevolumedocument.md)

## Governing policies

- <a id="pa-ab3228b5a1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.CollectionArchiveVolumeDocument.from_mapping`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ad9ce72b57a76ac884ee047cdf88f06e1b98084cac68577479841424991f974 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'object') -> 'CollectionArchiveVolumeDocument'\""
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "from_mapping",
  "owner": "riverhog_archive_contracts.CollectionArchiveVolumeDocument",
  "unit": "member"
}
```
