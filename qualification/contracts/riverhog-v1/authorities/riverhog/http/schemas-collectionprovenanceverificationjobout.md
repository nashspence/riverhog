# schemas: CollectionProvenanceVerificationJobOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionprovenanceverificationjobout:58529eb02d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-968608cbbf"></a>
- <a id="s-c514fe5c30"></a>`title`: CollectionProvenanceVerificationJobOut
- <a id="s-0e00b9ee87"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-47daac55a0"></a>`attempts` | yes | type="integer"; minimum=0 |  |
| <a id="s-6ca57b3926"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-b6bb0512f6"></a>`failure` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-c7a8ebe491"></a>`finished_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-f484534cb7"></a>`requested_at` | yes | type="string" |  |
| <a id="s-de3dc86efe"></a>`result` | yes | anyOf=#/components/schemas/CollectionProvenanceVerificationOut \| type="null" |  |
| <a id="s-e54a5477bb"></a>`started_at` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-32e51516fe"></a>`state` | yes | type="string"; enum=["queued","running","canceling","succeeded","failed","canceled"] |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: CollectionProvenanceVerificationOut](schemas-collectionprovenanceverificationout.md)

## Governing policies

- <a id="pa-cf38510bad"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionProvenanceVerificationJobOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de7cbad7331cf2f94febc0c3d07c3b32e066668ffe9796a2c2c66063f8e4cfc5 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "attempts": {
      "minimum": 0,
      "title": "Attempts",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "failure": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Failure"
    },
    "finished_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Finished At"
    },
    "requested_at": {
      "title": "Requested At",
      "type": "string"
    },
    "result": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionProvenanceVerificationOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "started_at": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Started At"
    },
    "state": {
      "enum": [
        "queued",
        "running",
        "canceling",
        "succeeded",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "state",
    "requested_at",
    "started_at",
    "finished_at",
    "attempts",
    "result",
    "failure"
  ],
  "title": "CollectionProvenanceVerificationJobOut",
  "type": "object"
}
```
