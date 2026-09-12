# schemas: RiverhogActor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-riverhogactor:77ecdb52af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-dfa081e99d"></a>
- <a id="s-b7b55d1149"></a>`title`: RiverhogActor
- <a id="s-3e08a408c6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3d49c0fa88"></a>`app` | yes | type="string"; minLength=1; maxLength=160 |  |
| <a id="s-dc7ca6a622"></a>`key_id` | no | anyOf=type="string"; minLength=1; maxLength=300 \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field app](#s-3d49c0fa88) | `length · characters · contract_max` | maximum=160 |
| <a id="s-200905861e"></a>[field key_id · string value](#s-dc7ca6a622) | `length · characters · contract_max` | maximum=300 |

## Governing policies

- <a id="pa-9ee8e812dd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a233993f8c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/RiverhogActor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae88be9c9fa5a2e60977b689011476674a5c673474d5e841bf78ebb369101905 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "app": {
      "maxLength": 160,
      "minLength": 1,
      "title": "App",
      "type": "string"
    },
    "key_id": {
      "anyOf": [
        {
          "maxLength": 300,
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Key Id"
    }
  },
  "required": [
    "app"
  ],
  "title": "RiverhogActor",
  "type": "object"
}
```
