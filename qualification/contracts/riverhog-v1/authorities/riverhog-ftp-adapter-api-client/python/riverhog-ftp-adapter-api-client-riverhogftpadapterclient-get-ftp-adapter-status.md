# riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.get_ftp_adapter_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-riverhogf-670c188fc3:8ee0873ae2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-339185e36d"></a>
- <a id="s-6b3c14ca55"></a>`distribution`: `riverhog-ftp-adapter-api-client`
- <a id="s-91686c2b51"></a>`module`: `riverhog_ftp_adapter_api_client`
- <a id="s-c9e222162f"></a>`name`: `get_ftp_adapter_status`
- <a id="s-c573dea8d6"></a>`owner`: `riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient`
- <a id="s-e05a1047cd"></a>`unit`: `member`

### Declared structure

- <a id="s-ae7494be69"></a>`kind`: `"method"`
- <a id="s-faa5c15ff9"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [riverhog-ftp-adapter status](../../riverhog-ftp-adapter/cli/riverhog-ftp-adapter-status.md)
- [GET /v1/status](../../riverhog-ftp-adapter/http-operations/get-v1-status.md)
- [RiverhogFtpAdapterClient](riverhog-ftp-adapter-api-client-riverhogftpadapterclient.md)

## Governing policies

- <a id="pa-7f3697cbda"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py`
- **Client method:** [reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py::RiverhogFtpAdapterClient.get_ftp_adapter_status](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py#L88)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.get_ftp_adapter_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bcb5d28f5f4fd9226b60ede91094d0a19f741a81fbef226f26d5081c03402402 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "get_ftp_adapter_status",
  "owner": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient",
  "unit": "member"
}
```

</details>
