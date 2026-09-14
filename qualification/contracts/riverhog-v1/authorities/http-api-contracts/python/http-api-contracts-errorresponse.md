# http_api_contracts.ErrorResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-errorresponse:62c8457bf7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-91b204b1ab"></a>
- <a id="s-40574b2b62"></a>`distribution`: `http-api-contracts`
- <a id="s-2848a82eae"></a>`module`: `http_api_contracts`
- <a id="s-10efc4a45b"></a>`name`: `ErrorResponse`
- <a id="s-860b8705fd"></a>`unit`: `export`

### Declared structure

- <a id="s-7a8fdbdd4c"></a>`kind`: `"class"`
- <a id="s-b9a54b73f4"></a>`signature`: `"'(*, error: http_api_contracts.ErrorBody) -> None'"`

#### Validated model schema

<a id="s-5b3e1a093f"></a>
- <a id="s-e6222fce40"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d48d2b6fd4"></a>`error` | yes | #/$defs/ErrorBody |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-7a9b318d90"></a>`ErrorBody` | type="object"; fields=`code`, `details`, `message`; additional keys=`additionalProperties`, `required` |

## Governing policies

- <a id="pa-78b812f1e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.ErrorResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f2473c9aeac6bd3592aabbba6b45f603d27be053426ba891ef601a2ae120a7bd -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ErrorBody": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "minLength": 1,
              "type": "string"
            },
            "details": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "message": {
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "error": {
          "$ref": "#/$defs/ErrorBody"
        }
      },
      "required": [
        "error"
      ],
      "type": "object"
    },
    "signature": "'(*, error: http_api_contracts.ErrorBody) -> None'"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "ErrorResponse",
  "unit": "export"
}
```
