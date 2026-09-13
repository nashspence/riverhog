# riverhog_ftp_adapter_api_client.HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-healthresponse:7116238a6f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-ftp-adapter-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-66acadb477"></a>
| Field | Shape |
|---|---|
| <a id="s-9edebfa6c4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-1da8c0b112"></a>`distribution` | "riverhog-ftp-adapter-api-client" |
| <a id="s-8e7d9a8a94"></a>`module` | "riverhog_ftp_adapter_api_client" |
| <a id="s-d93e03a287"></a>`name` | "HealthResponse" |
| <a id="s-0cd7f2fb27"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b74774511d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-ftp-adapter-api-client:riverhog_ftp_adapter_api_client](../../../evidence/sources.md#src-a83ae875ae) — `reference/riverhog/ingress/ftp-api-client/src/riverhog_ftp_adapter_api_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_ftp_adapter_api_client.HealthResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a096e555a76e0b271de3a8f75d2aa6cbabb33b26861922a9d2bba7b4f9af470c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31",
    "signature": "\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""
  },
  "distribution": "riverhog-ftp-adapter-api-client",
  "module": "riverhog_ftp_adapter_api_client",
  "name": "HealthResponse",
  "unit": "export"
}
```
