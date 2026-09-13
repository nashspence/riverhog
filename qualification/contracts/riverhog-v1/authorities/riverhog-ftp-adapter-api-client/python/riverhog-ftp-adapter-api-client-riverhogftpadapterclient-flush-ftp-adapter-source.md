# riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.flush_ftp_adapter_source

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-riverhogf-94ad188029:87531cabc1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d5883952d"></a>
| Field | Shape |
|---|---|
| <a id="s-b8480c34cf"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-7771f2c8fc"></a>`distribution` | "riverhog-ftp-adapter-api-client" |
| <a id="s-ab422f23d8"></a>`module` | "riverhog_ftp_adapter_api_client" |
| <a id="s-5057853792"></a>`name` | "flush_ftp_adapter_source" |
| <a id="s-45a16c29c9"></a>`owner` | "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient" |
| <a id="s-f385566b1a"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient](riverhog-ftp-adapter-api-client-riverhogftpadapterclient.md)

## Governing policies

- <a id="pa-693990a21e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient.flush_ftp_adapter_source`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30290204b9a27178ee65911c81bf524e24b52d85e3a0650fd9271c8e818ddf79 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source_id: 'str') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "flush_ftp_adapter_source",
  "owner": "riverhog_ftp_adapter_api_client.RiverhogFtpAdapterClient",
  "unit": "member"
}
```
