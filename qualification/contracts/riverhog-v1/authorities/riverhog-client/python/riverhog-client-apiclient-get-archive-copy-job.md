# riverhog_client.ApiClient.get_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-archive-copy-job:55529f3657 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c4d148755"></a>
- <a id="s-6b737f4b9a"></a>`distribution`: `riverhog-client`
- <a id="s-cb05a97037"></a>`module`: `riverhog_client`
- <a id="s-6b03f222f6"></a>`name`: `get_archive_copy_job`
- <a id="s-c46d343c0e"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-bb014bab75"></a>`unit`: `member`

### Declared structure

- <a id="s-8fba7e39be"></a>`kind`: `"method"`
- <a id="s-47f5d564ac"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli archive copy-job show](../../a-riverhog-cli/cli/a-riverhog-cli-archive-copy-job-show.md)
- [a-riverhog-cli archive copy-job watch](../../a-riverhog-cli/cli/a-riverhog-cli-archive-copy-job-watch.md)
- [GET /v1/archive/copy-jobs/{collection_id}/{destination_store}](../../riverhog/http-operations/get-v1-archive-copy-jobs-collection-id-destination-store.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-99862fb1fc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.get\_archive\_copy\_job](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2476)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_archive_copy_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e386a8f648c7352f466f94b5ec035f8896339012e1a4e023abfbf441f131dee6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_archive_copy_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
