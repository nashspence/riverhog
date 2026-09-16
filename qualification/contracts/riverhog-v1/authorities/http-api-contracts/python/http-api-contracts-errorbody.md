# http_api_contracts.ErrorBody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-errorbody:51f57b6cf2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-66f5e845ea"></a>
- <a id="s-a02aeebd1a"></a>`distribution`: `http-api-contracts`
- <a id="s-7bd38abf12"></a>`module`: `http_api_contracts`
- <a id="s-6b37ed451d"></a>`name`: `ErrorBody`
- <a id="s-1660f7149b"></a>`unit`: `export`

### Declared structure

- <a id="s-e81d9ad753"></a>`kind`: `"class"`
- <a id="s-8d39e038d5"></a>`signature`: `"'(*, code: Annotated[str, MinLen(min_length=1)], message: Annotated[str, MinLen(min_length=1)], details: dict[str, typing.Any] \| None = None) -> None'"`

#### Validated model schema

<a id="s-5887f8d888"></a>

- <a id="s-a088576b95"></a>`type`: `"object"`
- <a id="s-712aef54e9"></a>`additionalProperties`: `false`
- <a id="s-f0013763bb"></a>`required`: `["code","message"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-53d24d4f9b"></a>`code` | yes | type="string"; minLength=1 |  |
| <a id="s-e465fc5cf6"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-4da8dc85c7"></a>`message` | yes | type="string"; minLength=1 |  |

## Governing policies

- <a id="pa-d1eddee41c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources.md#src-a522df4cfd) — `packages/http-api-contracts/src/http_api_contracts/__init__.py`

### Machine authority

- `/external_contract/python/http_api_contracts.ErrorBody`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ded772624a4463a116a3a9b0eb5ba8c12998dc5973a306923ff0e872c41789fc -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
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
    },
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1)], message: Annotated[str, MinLen(min_length=1)], details: dict[str, typing.Any] | None = None) -> None'"
  },
  "distribution": "http-api-contracts",
  "module": "http_api_contracts",
  "name": "ErrorBody",
  "unit": "export"
}
```

</details>
