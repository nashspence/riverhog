# riverhog_archive_contracts.COLLECTION_ARCHIVE_VOLUME_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-collection-arc-eae0827cad:ac40f4f908 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f83462470"></a>
| Field | Shape |
|---|---|
| <a id="s-bae3d6440c"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-fb81bdc5f7"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-786182e924"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-4852dae34b"></a>`name` | "COLLECTION_ARCHIVE_VOLUME_SCHEMA" |
| <a id="s-907c8ec22b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-cdac002183"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.COLLECTION_ARCHIVE_VOLUME_SCHEMA`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: afd0f59d555e56a169acdd6b61b3e7f700671ae5e536249624ae193446e1b8d9 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "collection-archive-volume/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "COLLECTION_ARCHIVE_VOLUME_SCHEMA",
  "unit": "export"
}
```
