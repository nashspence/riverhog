# riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-riverhogf-5ae86d4a5b:c4da44a30e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d4ce809cd2"></a>
- <a id="s-cc63ea3472"></a>`distribution`: `riverhog-ftp-adapter-api-client`
- <a id="s-492b9b5e8a"></a>`module`: `riverhog_ftp_adapter_api_client`
- <a id="s-6f4c016411"></a>`name`: `RiverhogFtpAdapterClient`
- <a id="s-36b7d07849"></a>`unit`: `export`

### Declared structure

- <a id="s-14c83cd90a"></a>`kind`: `"class"`
- <a id="s-5ad2fd00d6"></a>`signature`: `"\"(base_url: 'str \| None' = None, token: 'str \| None' = None, *, allow_insecure_http: 'bool \| None' = None, timeout_seconds: 'float \| None' = None, http2: 'bool \| None' = None, transport: 'httpx.BaseTransport \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [__exit__](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-exit.md)
- [close](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-close.md)
- [__enter__](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-enter.md)
- [get_ftp_adapter_status](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-get-ftp-adapter-status.md)
- [flush_ftp_adapter_source](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-flush-ftp-adapter-source.md)
- [ftp_adapter_health_live](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-ftp-adapter-health-live.md)
- [run_ftp_adapter_pass](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-run-ftp-adapter-pass.md)
- [ftp_adapter_health_ready](riverhog-ftp-adapter-api-client-riverhogftpadapterclient-ftp-adapter-health-ready.md)

## Governing policies

- <a id="pa-f7ebb1a360"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources/authorities.md#src-a83ae875ae) — [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/\_\_init\_\_.py](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb2427a65ec019cbc30b4ce543f23909fcd264184a6f65abfabf1eb8cad77ca0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str | None' = None, token: 'str | None' = None, *, allow_insecure_http: 'bool | None' = None, timeout_seconds: 'float | None' = None, http2: 'bool | None' = None, transport: 'httpx.BaseTransport | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "RiverhogFtpAdapterClient",
  "unit": "export"
}
```

</details>
