# riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_live

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-riverhogf-9dfd9a367e:8a5f423dad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-257766f7b6"></a>
- <a id="s-10e6fbc444"></a>`distribution`: `riverhog-ftp-adapter-api-client`
- <a id="s-5ecb3f54c2"></a>`module`: `riverhog_ftp_adapter_api_client`
- <a id="s-d0f520e489"></a>`name`: `ftp_adapter_health_live`
- <a id="s-061fe6ce67"></a>`owner`: `riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient`
- <a id="s-5eba470b6f"></a>`unit`: `member`

### Declared structure

- <a id="s-e1b32784a1"></a>`kind`: `"method"`
- <a id="s-0ca758e0b6"></a>`signature`: `"\"(self) -> 'HealthResponse'\""`

## Maintained corroboration

### Related interface records

- [GET /health/live](../../riverhog-ftp-adapter/http-operations/get-health-live.md)
- [RiverhogFtpAdapterClient](riverhog-ftp-adapter-api-client-riverhogftpadapterclient.md)

## Governing policies

- <a id="pa-faf6fa16e2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/\_\_init\_\_.py](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py)
- **Client method:** [reference/riverhog/ingress/ftp-api-client/src/riverhog\_ftp\_adapter\_api\_client/client.py::RiverhogFtpAdapterClient.ftp\_adapter\_health\_live](../../../../../../reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/client.py#L80)

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.ftp_adapter_health_live`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d763d52dd51ab9c18a7e377c4d79618f4dd2b4a3334ef09f45f10f6e323eb48 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'HealthResponse'\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "ftp_adapter_health_live",
  "owner": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient",
  "unit": "member"
}
```

</details>
