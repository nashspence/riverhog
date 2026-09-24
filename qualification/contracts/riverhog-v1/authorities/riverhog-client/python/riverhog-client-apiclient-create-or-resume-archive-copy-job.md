# riverhog_client.ApiClient.create_or_resume_archive_copy_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-or-resum-d08f57d652:9ddb8c66ea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d350a864f8"></a>
- <a id="s-b53058796c"></a>`distribution`: `riverhog-client`
- <a id="s-064eb5521f"></a>`module`: `riverhog_client`
- <a id="s-aa9197cf56"></a>`name`: `create_or_resume_archive_copy_job`
- <a id="s-2c1f902b70"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-1dda9a0af8"></a>`unit`: `member`

### Declared structure

- <a id="s-b8d225081a"></a>`kind`: `"method"`
- <a id="s-29817e2866"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName', source_store: 'ArchiveStoreName \| None' = None, use_cache: 'bool \| None' = None, event_context: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [a-riverhog-cli archive copy-job start](../../a-riverhog-cli/cli/a-riverhog-cli-archive-copy-job-start.md)
- [POST /v1/archive/copy-jobs](../../riverhog/http-operations/post-v1-archive-copy-jobs.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-c996191b53"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.create\_or\_resume\_archive\_copy\_job](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2437)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_or_resume_archive_copy_job`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2b92213333ebb52bc82af1539503b050afd49fd9beb59e47f0f96dd459a0cc25 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, destination_store: 'ArchiveStoreName', source_store: 'ArchiveStoreName | None' = None, use_cache: 'bool | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_or_resume_archive_copy_job",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
