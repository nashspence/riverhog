# riverhog_client.ApiClient.get_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-collection-d1c921e752:43c7a64c49 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-30ec73c02f"></a>
- <a id="s-036cf4fdbe"></a>`distribution`: `riverhog-client`
- <a id="s-78678ffa58"></a>`module`: `riverhog_client`
- <a id="s-3d95511302"></a>`name`: `get_collection_upload_session_provenance_journal`
- <a id="s-e8c03edceb"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-a62226ff42"></a>`unit`: `member`

### Declared structure

- <a id="s-28751641bc"></a>`kind`: `"method"`
- <a id="s-914b46cdc1"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'CollectionUploadProvenanceJournalStatusDocument'\""`

## Maintained corroboration

### Related interface records

- [GET /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](../../riverhog/http-operations/get-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-4f61122d85"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.get_collection_upload_session_provenance_journal](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1321)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_collection_upload_session_provenance_journal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d777beda2f75885f777e87b79f363485ab97984ca2c6f547b3a7425754d53135 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'CollectionUploadProvenanceJournalStatusDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_collection_upload_session_provenance_journal",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
