# riverhog_client.ApiClient.create_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-collecti-4a44cdde10:35754b3c43 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7295015261"></a>
- <a id="s-f7220f9c25"></a>`distribution`: `riverhog-client`
- <a id="s-3779d660a8"></a>`module`: `riverhog_client`
- <a id="s-28084ab14d"></a>`name`: `create_collection_upload_session_provenance_journal`
- <a id="s-b2351268f7"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-769ddfdf92"></a>`unit`: `member`

### Declared structure

- <a id="s-df0a3fc299"></a>`kind`: `"method"`
- <a id="s-73f7c65846"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, byte_count: 'int', sha256: 'str') -> 'CollectionUploadProvenanceJournalStatusDocument'\""`

## Maintained corroboration

### Related interface records

- [PUT /v1/collection-upload-sessions/{collection_id}/provenance/journals/{journal_id}](../../riverhog/http-operations/put-v1-collection-upload-sessions-collection-id-provenance-journals-journal-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f76c5ece64"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.create\_collection\_upload\_session\_provenance\_journal](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1274)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_collection_upload_session_provenance_journal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
