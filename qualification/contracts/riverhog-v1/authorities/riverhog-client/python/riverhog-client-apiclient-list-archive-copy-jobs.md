# riverhog_client.ApiClient.list_archive_copy_jobs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-archive-copy-jobs:b9cc6c3722 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-40c6a76c39"></a>
- <a id="s-8b1dc566b7"></a>`distribution`: `riverhog-client`
- <a id="s-f28f170d8f"></a>`module`: `riverhog_client`
- <a id="s-07738dcd36"></a>`name`: `list_archive_copy_jobs`
- <a id="s-ceadf12a0b"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-e375de2103"></a>`unit`: `member`

### Declared structure

- <a id="s-e0e56d772e"></a>`kind`: `"method"`
- <a id="s-0de41dc32e"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None, state: 'ArchiveCopyState \| None' = None, sort: 'ArchiveCopySort' = 'requested_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity archive copy list](../../piggity/cli/piggity-archive-copy-list.md)
- [GET /v1/archive/copies](../../riverhog/http-operations/get-v1-archive-copies.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-9089992b92"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_archive\_copy\_jobs](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2438)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_archive_copy_jobs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe5cf182e9a28915c3c1cb5e5c39d259567fe5db38e43508176273fbcc37d3e8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, state: 'ArchiveCopyState | None' = None, sort: 'ArchiveCopySort' = 'requested_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_archive_copy_jobs",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
