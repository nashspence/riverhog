# riverhog_client.ApiClient.list_collection_provenance_journal_agents

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-6bb73a497b:8fd4e5b490 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ddd6168db0"></a>
- <a id="s-35f2a45e8b"></a>`distribution`: `riverhog-client`
- <a id="s-8459cb5154"></a>`module`: `riverhog_client`
- <a id="s-10e5b035b4"></a>`name`: `list_collection_provenance_journal_agents`
- <a id="s-f83db5b423"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-39b341a898"></a>`unit`: `member`

### Declared structure

- <a id="s-a7b708e030"></a>`kind`: `"method"`
- <a id="s-8cc9f0213d"></a>`signature`: `"\"(self, collection_id: 'CollectionId', journal_id: 'ProvenanceJournalId', *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli collection provenance agents](../../a-riverhog-cli/cli/a-riverhog-cli-collection-provenance-agents.md)
- [GET /v1/collections/{collection_id}/provenance/journals/{journal_id}/agents](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-journals-journal-id-agents.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-c116b6ff6c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_collection\_provenance\_journal\_agents](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1919)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_provenance_journal_agents`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
