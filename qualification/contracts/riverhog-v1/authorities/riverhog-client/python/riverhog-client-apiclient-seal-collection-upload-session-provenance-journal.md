# riverhog_client.ApiClient.seal_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-seal-collection-dc5a0f3853:d20820874c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d678e771d6"></a>
- <a id="s-98a01480c1"></a>`distribution`: `riverhog-client`
- <a id="s-3cad440110"></a>`module`: `riverhog_client`
- <a id="s-898313dbb4"></a>`name`: `seal_collection_upload_session_provenance_journal`
- <a id="s-ee80e56106"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-0bf5970528"></a>`unit`: `member`

### Declared structure

- <a id="s-494fe7d2a4"></a>`kind`: `"method"`
- <a id="s-ce26596c46"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'CollectionUploadProvenanceJournalStatusDocument'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-856d964d14"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.seal_collection_upload_session_provenance_journal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c82ded4c90196e5d69222bb06963f2590a4a32fb85f1d6a8c75c7d234babfb02 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'CollectionUploadProvenanceJournalStatusDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "seal_collection_upload_session_provenance_journal",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
