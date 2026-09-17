# schemas: ArtifactDispositionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositiondocument:33ba83ec15 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-24a8ecfb85"></a>

- <a id="s-517aea0476"></a>`type`: `"object"`
- <a id="s-a26fad90e1"></a>`additionalProperties`: `false`
- <a id="s-d0cf7e1771"></a>`required`: `["input","status"]`
- <a id="s-48f677b91a"></a>`title`: `"ArtifactDispositionDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f848c33494"></a>`failure` | no | anyOf=[([ArtifactDispositionFailureDocument](schemas-artifactdispositionfailuredocument.md)); (type="null")] |  |
| <a id="s-f7c6e570c3"></a>`input` | yes | [ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md) |  |
| <a id="s-f506733223"></a>`status` | yes | type="string"; enum=["transformed","preserved","omitted","rejected"]; title="Status" |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| 1 | [See `oneOf` alternative 1](#s-27b164a074) |
| 2 | [See `oneOf` alternative 2](#s-0bdc389c28) |

### <a id="s-27b164a074"></a>`oneOf` alternative 1


#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-24fed008fd"></a>`failure` | no | type="null" |  |
| <a id="s-e8c4ca74e4"></a>`status` | no | enum=["transformed","preserved"] |  |

### <a id="s-0bdc389c28"></a>`oneOf` alternative 2

- <a id="s-e3fd6b295c"></a>`required`: `["failure"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8abf5c80d2"></a>`failure` | yes | type="object" |  |
| <a id="s-2a7f6bc55d"></a>`status` | no | enum=["omitted","rejected"] |  |

## Maintained corroboration

### Referenced contract elements

- [ArtifactDispositionFailureDocument](schemas-artifactdispositionfailuredocument.md)
- [ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md)

## Governing policies

- <a id="pa-0a8e06745d"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: edfafa82144c8be20244c30934ad87c7fee86c5237da936cff0d58ef40a5a48c -->

```json
{
  "additionalProperties": false,
  "oneOf": [
    {
      "properties": {
        "failure": {
          "type": "null"
        },
        "status": {
          "enum": [
            "transformed",
            "preserved"
          ]
        }
      }
    },
    {
      "properties": {
        "failure": {
          "type": "object"
        },
        "status": {
          "enum": [
            "omitted",
            "rejected"
          ]
        }
      },
      "required": [
        "failure"
      ]
    }
  ],
  "properties": {
    "failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArtifactDispositionFailureDocument"
        },
        {
          "type": "null"
        }
      ]
    },
    "input": {
      "$ref": "#/components/schemas/ArtifactDispositionInputDocument"
    },
    "status": {
      "enum": [
        "transformed",
        "preserved",
        "omitted",
        "rejected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "input",
    "status"
  ],
  "title": "ArtifactDispositionDocument",
  "type": "object"
}
```

</details>
