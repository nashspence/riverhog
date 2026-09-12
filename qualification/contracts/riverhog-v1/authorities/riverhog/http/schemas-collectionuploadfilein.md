# schemas: CollectionUploadFileIn

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadfilein:38d42c5c93 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 2 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadFileIn`

## Effective policies

- `compatibility/http-api/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: CanonicalRelPath](schemas-canonicalrelpath.md)
- [schemas: CapturedFileProvenanceBinding](schemas-capturedfileprovenancebinding.md)
- [schemas: CollectionUploadRawPartsIn](schemas-collectionuploadrawpartsin.md)
- [schemas: OmittedFileProvenanceBinding](schemas-omittedfileprovenancebinding.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `title`: CollectionUploadFileIn
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `bytes` | yes | integer |  |
| `path` | yes | #/components/schemas/CanonicalRelPath |  |
| `provenance` | no | object (2 fields) |  |
| `raw_parts` | no | object (1 fields) |  |
| `sha256` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e7d4e892dc37bf368acdc769e28c09f91487ce087943be592b05aabcd3e6a16 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "bytes": {
      "minimum": 0,
      "title": "Bytes",
      "type": "integer"
    },
    "path": {
      "$ref": "#/components/schemas/CanonicalRelPath"
    },
    "provenance": {
      "anyOf": [
        {
          "discriminator": {
            "mapping": {
              "captured": "#/components/schemas/CapturedFileProvenanceBinding",
              "omitted": "#/components/schemas/OmittedFileProvenanceBinding"
            },
            "propertyName": "status"
          },
          "oneOf": [
            {
              "$ref": "#/components/schemas/CapturedFileProvenanceBinding"
            },
            {
              "$ref": "#/components/schemas/OmittedFileProvenanceBinding"
            }
          ]
        },
        {
          "type": "null"
        }
      ],
      "title": "Provenance"
    },
    "raw_parts": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionUploadRawPartsIn"
        },
        {
          "type": "null"
        }
      ]
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Sha256",
      "type": "string"
    }
  },
  "required": [
    "path",
    "bytes",
    "sha256"
  ],
  "title": "CollectionUploadFileIn",
  "type": "object"
}
```
