# riverhog_client.ApiClient.upload_collection_upload_session_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-upload-collecti-8685cc31c7:8c06b54c1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8802f7898a"></a>
- <a id="s-726529f56a"></a>`distribution`: `riverhog-client`
- <a id="s-7bbeb6180f"></a>`module`: `riverhog_client`
- <a id="s-b8858cff39"></a>`name`: `upload_collection_upload_session_provenance_journal`
- <a id="s-a921671596"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-c9bb5eb8d0"></a>`unit`: `member`

### Declared structure

- <a id="s-2296e85153"></a>`kind`: `"method"`
- <a id="s-ce1ba457fd"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, content: 'Iterable[bytes]', byte_count: 'int', sha256: 'str') -> 'CollectionUploadProvenanceJournalStatusDocument'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6b2051b383"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.upload_collection_upload_session_provenance_journal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd0a91a65fe018aa726fc2513934ecf974c33bb13abda481f2521f0eb756b2bd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, content: 'Iterable[bytes]', byte_count: 'int', sha256: 'str') -> 'CollectionUploadProvenanceJournalStatusDocument'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "upload_collection_upload_session_provenance_journal",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
