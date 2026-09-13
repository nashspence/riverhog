# riverhog_client.ApiClient.append_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-append-collecti-2050f14c5e:5e4cf0d89e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-38c078fe60"></a>
| Field | Shape |
|---|---|
| <a id="s-903b56bb39"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9c6033fbba"></a>`distribution` | "riverhog-client" |
| <a id="s-ea4acccf2b"></a>`module` | "riverhog_client" |
| <a id="s-1d431059f4"></a>`name` | "append_collection_upload_session_provenance_journal" |
| <a id="s-400bea03c4"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-3ac3d42ce5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-188925ce30"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.append_collection_upload_session_provenance_journal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: eec23607d6eb1ab61282dc5f5b33767d9456c7c0e90054403ba2019900d663cb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, offset: 'int', content: 'bytes') -> 'CollectionUploadProvenanceJournalStatusDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "append_collection_upload_session_provenance_journal",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
