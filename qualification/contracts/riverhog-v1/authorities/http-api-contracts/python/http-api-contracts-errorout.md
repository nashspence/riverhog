# http_api_contracts.ErrorOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:http-api-contracts:http-api-contracts-errorout:0eab59926a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [http-api-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4cb94eb9d0"></a>
- <a id="s-62b11a2249"></a>`distribution`: `http-api-contracts`
- <a id="s-3e51b44207"></a>`module`: `http_api_contracts`
- <a id="s-2fe34d1ff5"></a>`name`: `ErrorOut`
- <a id="s-4deb908202"></a>`unit`: `export`

### Declared structure

- <a id="s-f2e576d0e5"></a>`kind`: `"class"`
- <a id="s-4df9a0cf0f"></a>`signature`: `"'(*, error: http_api_contracts.ErrorBody) -> None'"`

#### Validated model schema

<a id="s-aecff05a5e"></a>

- <a id="s-841cc14cf9"></a>`type`: `"object"`
- <a id="s-30d20a3dab"></a>`additionalProperties`: `false`
- <a id="s-a092118255"></a>`required`: `["error"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8dee9315c2"></a>`error` | yes | [ErrorBody](#s-4d3b0c4e39) |  |

##### Definitions

- [ErrorBody](#s-4d3b0c4e39)

##### <a id="s-4d3b0c4e39"></a>definition `ErrorBody`

- <a id="s-8eabe04f16"></a>`type`: `"object"`
- <a id="s-5d93d837d3"></a>`additionalProperties`: `false`
- <a id="s-fa138da8c2"></a>`required`: `["code","message"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-26f98fea35"></a>`code` | yes | type="string"; minLength=1 |  |
| <a id="s-fbfe9b7363"></a>`details` | no | anyOf=[(type="object"; additionalProperties=(any JSON value)); (type="null")]; default=null |  |
| <a id="s-6f5c9acc61"></a>`message` | yes | type="string"; minLength=1 |  |

## Governing policies

- <a id="pa-a988a45f8e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:http-api-contracts:http_api_contracts](../../../evidence/sources/authorities.md#src-a522df4cfd) — [packages/http-api-contracts/src/http\_api\_contracts/\_\_init\_\_.py](../../../../../../packages/http-api-contracts/src/http_api_contracts/__init__.py)

### Machine authority

- `/external_contract/python/http_api_contracts.ErrorOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25941521920ef79903a9c9c121d55e36e360ff50fc2d73e8b1c5ee883021b0cb -->

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
  "name": "ErrorOut",
  "unit": "export"
}
```

</details>
