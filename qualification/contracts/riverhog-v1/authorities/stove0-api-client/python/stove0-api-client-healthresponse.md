# stove0_api_client.HealthResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-api-client:stove0-api-client-healthresponse:593bd942d2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91879d1fe9"></a>
| Field | Shape |
|---|---|
| <a id="s-fcd6977b97"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-0ef0fc56a1"></a>`distribution` | "stove0-api-client" |
| <a id="s-9b0487570c"></a>`module` | "stove0_api_client" |
| <a id="s-3a58591061"></a>`name` | "HealthResponse" |
| <a id="s-bef0b63b53"></a>`unit` | "export" |

## Governing policies

- <a id="pa-39ff579dc4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-api-client:stove0_api_client](../../../evidence/sources.md#src-5d52ac5998) — `reference/stove0/packages/api-client/src/stove0_api_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_api_client.HealthResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c802b812bfdee54b26ab9af83212281fa40c81d1b03f193e1837ff432add37e -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "873f58b65973a85d82bd4e352acd595a8f32f6058c4500f11514358669b42b31",
    "signature": "\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "HealthResponse",
  "unit": "export"
}
```
