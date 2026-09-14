# riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_ready

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-riverhogf-ed2a102068:44729cdd22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-869bb3bfa0"></a>
- <a id="s-e61e8442ea"></a>`distribution`: `riverhog-ftp-adapter-api-client`
- <a id="s-f3d0dfe9a7"></a>`module`: `riverhog_ftp_adapter_api_client`
- <a id="s-472d5c96d1"></a>`name`: `ftp_adapter_health_ready`
- <a id="s-41e4ed0ef0"></a>`owner`: `riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient`
- <a id="s-c6b5019dc3"></a>`unit`: `member`

### Declared structure

- <a id="s-9949701a92"></a>`kind`: `"method"`
- <a id="s-51327af1cf"></a>`signature`: `"\"(self) -> 'HealthResponse'\""`

## Maintained corroboration

### Related interface records

- [RiverhogFtpAdapterClient](riverhog-ftp-adapter-api-client-riverhogftpadapterclient.md)

## Governing policies

- <a id="pa-33085fd92c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_ready`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc6883ce901fd1bb7b4f654cc8aa7377f01956ef059bf9c3cd424784fe3279da -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'HealthResponse'\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "ftp_adapter_health_ready",
  "owner": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient",
  "unit": "member"
}
```
