# riverhog_client.ApiClient.list_download_quotas

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-download-quotas:e549c4b18e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2c23cc06f"></a>
- <a id="s-111d15c433"></a>`distribution`: `riverhog-client`
- <a id="s-c8ba915997"></a>`module`: `riverhog_client`
- <a id="s-dbe005703e"></a>`name`: `list_download_quotas`
- <a id="s-d25982b5d3"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-2ea75bcfd1"></a>`unit`: `member`

### Declared structure

- <a id="s-e73beb5b40"></a>`kind`: `"method"`
- <a id="s-76f221c1cf"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None, sort: 'DownloadQuotaSort' = 'app', order: 'SortOrder' = 'asc', app: 'ApplicationName \| None' = None, active: 'bool \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key quota list](../../piggity/cli/piggity-app-key-quota-list.md)
- [GET /v1/download-quotas](../../riverhog/http-operations/get-v1-download-quotas.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-f44d806336"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_download\_quotas](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2381)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_download_quotas`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba125f7085670fdcd941544b0e176d9caa22a2c3d9060c4090c223f322528db7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'DownloadQuotaSort' = 'app', order: 'SortOrder' = 'asc', app: 'ApplicationName | None' = None, active: 'bool | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_download_quotas",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
