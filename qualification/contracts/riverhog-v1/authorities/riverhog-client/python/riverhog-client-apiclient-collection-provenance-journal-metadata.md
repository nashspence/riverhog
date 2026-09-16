# riverhog_client.ApiClient.collection_provenance_journal_metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-collection-prov-65333d2e6d:0219b5da62 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b50f8b95e4"></a>
- <a id="s-9ce69f0efe"></a>`distribution`: `riverhog-client`
- <a id="s-c717a1105d"></a>`module`: `riverhog_client`
- <a id="s-ab27e744e5"></a>`name`: `collection_provenance_journal_metadata`
- <a id="s-4c478e5bf9"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-df88a54f38"></a>`unit`: `member`

### Declared structure

- <a id="s-d7ad9a33dc"></a>`kind`: `"method"`
- <a id="s-f16d48e3bb"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'tuple[int, str]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b7abe5b551"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.collection_provenance_journal_metadata`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd50f0d4f0480de9201ebdb00423c34b54a5289c6796894b73277d4cacb5f127 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId') -> 'tuple[int, str]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "collection_provenance_journal_metadata",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
