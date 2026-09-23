# riverhog_client.ApiClient.cancel_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-cancel-archive-copy-job:2c86d297eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-529b0068f3"></a>
- <a id="s-b15e37dc9c"></a>`distribution`: `riverhog-client`
- <a id="s-a6024983ec"></a>`module`: `riverhog_client`
- <a id="s-317cd44ba0"></a>`name`: `cancel_archive_copy_job`
- <a id="s-e9d5fa759a"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-f4d3e79aeb"></a>`unit`: `member`

### Declared structure

- <a id="s-42a000275a"></a>`kind`: `"method"`
- <a id="s-955013a445"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity archive copy cancel](../../piggity/cli/piggity-archive-copy-cancel.md)
- [DELETE /v1/archive/copies/{collection_id}/{destination_store}](../../riverhog/http-operations/delete-v1-archive-copies-collection-id-destination-store.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6686a6a0fe"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.cancel\_archive\_copy\_job](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2489)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.cancel_archive_copy_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5b045bebdfc73b52fcd9ffc7b26975d03d98115ff2fcf1ae46e4e8b9873c067 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "cancel_archive_copy_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
