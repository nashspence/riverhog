# riverhog_client.ApiClient.set_app_key_download_quota

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-set-app-key-download-quota:7c7ee04d42 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-491a1046f9"></a>
- <a id="s-818d943d1e"></a>`distribution`: `riverhog-client`
- <a id="s-3b891d582d"></a>`module`: `riverhog_client`
- <a id="s-4a40b17724"></a>`name`: `set_app_key_download_quota`
- <a id="s-9c5a658696"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-c834b1c486"></a>`unit`: `member`

### Declared structure

- <a id="s-1561bbdeb7"></a>`kind`: `"method"`
- <a id="s-7c55857d6d"></a>`signature`: `"\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, monthly_bytes: 'MonthlyDownloadQuotaBytes \| None') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key quota set](../../piggity/cli/piggity-app-key-quota-set.md)
- [PUT /v1/apps/{app}/keys/{key_id}/download-quota](../../riverhog/http-operations/put-v1-apps-app-keys-key-id-download-quota.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-75edfe7e48"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.set\_app\_key\_download\_quota](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2355)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.set_app_key_download_quota`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b7a1020e1c8129a1afe6300a46186b6590a7406536ef49df056fae4548d6ef4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', key_id: 'ApplicationKeyId', *, monthly_bytes: 'MonthlyDownloadQuotaBytes | None') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "set_app_key_download_quota",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
