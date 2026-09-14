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
- <a id="s-0ef0fc56a1"></a>`distribution`: `stove0-api-client`
- <a id="s-9b0487570c"></a>`module`: `stove0_api_client`
- <a id="s-3a58591061"></a>`name`: `HealthResponse`
- <a id="s-bef0b63b53"></a>`unit`: `export`

### Declared structure

- <a id="s-1304a11421"></a>`kind`: `"class"`
- <a id="s-b30f38cc70"></a>`signature`: `"\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""`

#### Validated model schema

<a id="s-d70ecaecc6"></a>
- <a id="s-1dcc028803"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8f1bae9549"></a>`service` | yes | type="string"; minLength=1 |  |
| <a id="s-28c3bc43e4"></a>`status` | yes | type="string"; const="ok" |  |

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

<!-- exact-contract-value: 9b3119f67413721e4b6e2bc51cc76144e515376f29419056a9ad30aa71b423b6 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "service": {
          "minLength": 1,
          "type": "string"
        },
        "status": {
          "const": "ok",
          "type": "string"
        }
      },
      "required": [
        "service",
        "status"
      ],
      "type": "object"
    },
    "signature": "\"(*, service: Annotated[str, MinLen(min_length=1)], status: Literal['ok']) -> None\""
  },
  "distribution": "stove0-api-client",
  "module": "stove0_api_client",
  "name": "HealthResponse",
  "unit": "export"
}
```
