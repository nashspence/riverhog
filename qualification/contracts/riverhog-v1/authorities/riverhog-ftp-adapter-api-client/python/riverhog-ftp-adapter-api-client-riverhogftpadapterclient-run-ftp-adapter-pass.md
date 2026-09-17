# riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.run_ftp_adapter_pass

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-riverhogf-d51bf55c0a:63b974c7c4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-613b01b51a"></a>
- <a id="s-a9fe727bf3"></a>`distribution`: `riverhog-ftp-adapter-api-client`
- <a id="s-7b7faf96cd"></a>`module`: `riverhog_ftp_adapter_api_client`
- <a id="s-d64b7f4379"></a>`name`: `run_ftp_adapter_pass`
- <a id="s-bf566055e9"></a>`owner`: `riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient`
- <a id="s-e6cccd835a"></a>`unit`: `member`

### Declared structure

- <a id="s-0344459704"></a>`kind`: `"method"`
- <a id="s-edb042541a"></a>`signature`: `"\"(self) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [riverhog-ftp-adapter run](../../riverhog-ftp-adapter/cli/riverhog-ftp-adapter-run.md)
- [POST /v1/run](../../riverhog-ftp-adapter/http-operations/post-v1-run.md)
- [RiverhogFtpAdapterClient](riverhog-ftp-adapter-api-client-riverhogftpadapterclient.md)

## Governing policies

- <a id="pa-42a4bbf1b4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources/authorities.md#src-a83ae875ae) — [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/\_\_init\_\_.py](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py)
- **Client method:** [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py::RiverhogFtpAdapterClient.run\_ftp\_adapter\_pass](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py#L99)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.run_ftp_adapter_pass`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3d85263ae53404d1223cd0d16068d0dd435b4603607a52597dcfdd880ae4b5b4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "run_ftp_adapter_pass",
  "owner": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient",
  "unit": "member"
}
```

</details>
