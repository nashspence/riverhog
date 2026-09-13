# riverhog_ftp_adapter_api_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client:ff4633836e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-6667446192) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d78d1a0365"></a>
| Field | Shape |
|---|---|
| <a id="s-f095da1d9b"></a>`candidate_id` | "python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client" |
| <a id="s-2679979a1a"></a>`distribution` | "riverhog-ftp-adapter-api-client" |
| <a id="s-55ec319891"></a>`exports` | additional keys=`FtpAdapterApiError`, `HealthResponse`, `RiverhogFtpAdapterClient` |
| <a id="s-23239c93e3"></a>`module` | "riverhog_ftp_adapter_api_client" |

## Governing policies

- <a id="pa-4854fe9345"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py`

### Machine authority

- `/external_contract/python/16`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 077746bc9c86951f72cdc3718cb9906689226cf6d56486ee13d51ed456597058 -->

```json
{
  "candidate_id": "python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client",
  "distribution": "riverhog-ftp-adapter-api-client",
  "exports": {
    "FtpAdapterApiError": {
      "kind": "class",
      "signature": "\"(message: 'str', *, code: 'str' = 'ftp_adapter_error', status: 'int | None' = None) -> 'None'\""
    },
    "HealthResponse": {
      "kind": "class",
      "schema_sha256": "873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31",
      "signature": "\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""
    },
    "RiverhogFtpAdapterClient": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "flush_ftp_adapter_source": {
          "kind": "method",
          "signature": "\"(self, source_id: 'str') -> 'dict[str, Any]'\""
        },
        "ftp_adapter_health_live": {
          "kind": "method",
          "signature": "\"(self) -> 'HealthResponse'\""
        },
        "ftp_adapter_health_ready": {
          "kind": "method",
          "signature": "\"(self) -> 'HealthResponse'\""
        },
        "get_ftp_adapter_status": {
          "kind": "method",
          "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
        },
        "run_ftp_adapter_pass": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, Any]'\""
        }
      },
      "signature": "\"(base_url: 'str | None' = None, token: 'str | None' = None, *, allow_insecure_http: 'bool | None' = None, timeout_seconds: 'float | None' = None, http2: 'bool | None' = None, transport: 'httpx.BaseTransport | None' = None) -> 'None'\""
    }
  },
  "module": "riverhog_ftp_adapter_api_client"
}
```
