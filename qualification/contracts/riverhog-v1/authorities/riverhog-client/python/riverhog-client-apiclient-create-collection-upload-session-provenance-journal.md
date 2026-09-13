# riverhog_client.ApiClient.create_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-collecti-4a44cdde10:35754b3c43 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7295015261"></a>
| Field | Shape |
|---|---|
| <a id="s-d1bd75ffc0"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f7220f9c25"></a>`distribution` | "riverhog-client" |
| <a id="s-3779d660a8"></a>`module` | "riverhog_client" |
| <a id="s-28084ab14d"></a>`name` | "create_collection_upload_session_provenance_journal" |
| <a id="s-b2351268f7"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-769ddfdf92"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f76c5ece64"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_collection_upload_session_provenance_journal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9234ca523087f68a76e2025c4586a7c7b7ef09d6f50fee943cc62b4ca753bbbd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, byte_count: 'int', sha256: 'str') -> 'CollectionUploadProvenanceJournalStatusDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_collection_upload_session_provenance_journal",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
