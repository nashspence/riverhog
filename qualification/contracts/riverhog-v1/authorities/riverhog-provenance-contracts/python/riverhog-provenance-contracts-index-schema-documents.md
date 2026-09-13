# riverhog_provenance_contracts.index_schema_documents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance-contracts:riverhog-provenance-contracts-index-schema-documents:8c7e47ce6d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d90e69db73"></a>
| Field | Shape |
|---|---|
| <a id="s-0bba640ee7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-73e77e41fc"></a>`distribution` | "riverhog-provenance-contracts" |
| <a id="s-5e260c8aa9"></a>`module` | "riverhog_provenance_contracts" |
| <a id="s-2d395b29d8"></a>`name` | "index_schema_documents" |
| <a id="s-842ec8bb83"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3e86c87807"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance-contracts:riverhog_provenance_contracts](../../../evidence/sources.md#src-9b6289a988) — `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance_contracts.index_schema_documents`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4cd94a467f612cb4bc6d3e956e43d411a6ee8011b34b1458860282b168379fa8 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(documents: 'Iterable[Mapping[str, Any]]', *, owner: 'str') -> 'dict[str, dict[str, Any]]'\""
  },
  "distribution": "riverhog-provenance-contracts",
  "module": "riverhog_provenance_contracts",
  "name": "index_schema_documents",
  "unit": "export"
}
```
