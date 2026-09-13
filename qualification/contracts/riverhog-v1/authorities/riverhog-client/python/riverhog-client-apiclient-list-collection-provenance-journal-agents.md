# riverhog_client.ApiClient.list_collection_provenance_journal_agents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-6bb73a497b:8fd4e5b490 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ddd6168db0"></a>
| Field | Shape |
|---|---|
| <a id="s-00dc1d4ac8"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-35f2a45e8b"></a>`distribution` | "riverhog-client" |
| <a id="s-8459cb5154"></a>`module` | "riverhog_client" |
| <a id="s-10e5b035b4"></a>`name` | "list_collection_provenance_journal_agents" |
| <a id="s-f83db5b423"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-39b341a898"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-c116b6ff6c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_provenance_journal_agents`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cac084999feed6dbd76a669257a3844473286e5618bf103d9bb47a001997ff25 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_provenance_journal_agents",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
