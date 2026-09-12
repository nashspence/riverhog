# schemas: ArtifactDispositionDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositiondocument:9ebd40c640 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-24a8ecfb8537"></a>
- <a id="s-48f677b91a89"></a>`title`: ArtifactDispositionDocument
- <a id="s-517aea0476f2"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f848c334947f"></a>`failure` | no | anyOf=#/components/schemas/ArtifactDispositionFailureDocument \| type="null" |  |
| <a id="s-f7c6e570c32f"></a>`input` | yes | #/components/schemas/ArtifactDispositionInputDocument |  |
| <a id="s-f50673322391"></a>`status` | yes | type="string"; enum=["transformed","preserved","omitted","rejected"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionFailureDocument](schemas-artifactdispositionfailuredocument.md)
- [schemas: ArtifactDispositionInputDocument](schemas-artifactdispositioninputdocument.md)

## Governing policies

- <a id="pa-7371be968e7d"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionDocument`

### Exact owned JSON

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
