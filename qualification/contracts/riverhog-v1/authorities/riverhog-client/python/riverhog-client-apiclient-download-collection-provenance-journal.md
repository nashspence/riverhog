# riverhog_client.ApiClient.download_collection_provenance_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-download-collec-e07f299bbc:6a372842ad -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8b388893cb"></a>
- <a id="s-c7fb125c36"></a>`distribution`: `riverhog-client`
- <a id="s-c553abb203"></a>`module`: `riverhog_client`
- <a id="s-24dba0e749"></a>`name`: `download_collection_provenance_journal`
- <a id="s-defdaa85c0"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-3d9ee906dd"></a>`unit`: `member`

### Declared structure

- <a id="s-0eb0e84911"></a>`kind`: `"method"`
- <a id="s-928c7743a3"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, output: 'Path') -> 'tuple[int, str]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-854f2d092f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.download_collection_provenance_journal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8986c2b6cfaf1a5ea5e5cc0b56535c6e6aba1f00df6aeac27882640fe6e30282 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, output: 'Path') -> 'tuple[int, str]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "download_collection_provenance_journal",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
