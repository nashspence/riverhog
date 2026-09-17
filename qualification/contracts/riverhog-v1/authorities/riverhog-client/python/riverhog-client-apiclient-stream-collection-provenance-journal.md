# riverhog_client.ApiClient.stream_collection_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-stream-collecti-903bed7a28:cef2214529 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ef08386f9d"></a>
- <a id="s-251c3274ea"></a>`distribution`: `riverhog-client`
- <a id="s-a162b7b9a7"></a>`module`: `riverhog_client`
- <a id="s-45e88dfd2f"></a>`name`: `stream_collection_provenance_journal`
- <a id="s-56a4b56712"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-43ebfce0f0"></a>`unit`: `member`

### Declared structure

- <a id="s-4b13f660d0"></a>`kind`: `"method"`
- <a id="s-7f6001adfd"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, start: 'int' = 0, end: 'int \| None' = None, expected_bytes: 'int \| None' = None, expected_sha256: 'str \| None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection provenance export](../../piggity/cli/piggity-collection-provenance-export.md)
- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-journals-journal-id.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-606ffa5e5d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.stream\_collection\_provenance\_journal](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1683)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.stream_collection_provenance_journal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91113cf5dddb5540a787e7d3574a0a86840c4390bee1b0e6083ee11f22254266 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, start: 'int' = 0, end: 'int | None' = None, expected_bytes: 'int | None' = None, expected_sha256: 'str | None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "stream_collection_provenance_journal",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
