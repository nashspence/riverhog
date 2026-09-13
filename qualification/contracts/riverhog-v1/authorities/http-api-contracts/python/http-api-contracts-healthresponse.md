# http_api_contracts.HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-healthresponse:15b925f3ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c086949aac"></a>
| Field | Shape |
|---|---|
| <a id="s-27bda23f3f"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-61740d85b0"></a>`distribution` | "http-api-contracts" |
| <a id="s-a0ee19967c"></a>`module` | "http_api_contracts" |
| <a id="s-38e35b295a"></a>`name` | "HealthResponse" |
| <a id="s-3898c814ba"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9a3e812c6a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.HealthResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fbf28f0cbfd13a115985825a3c520b07c65a27348d5d2d4ffc693e99b0df8cd7 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31",
    "signature": "\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "HealthResponse",
  "unit": "export"
}
```
