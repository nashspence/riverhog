# schemas: CollectionFinalizedData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionfinalizeddata:62b28ef6ec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-99b0948ee4"></a>

- <a id="s-0b32ac852c"></a>`type`: `"object"`
- <a id="s-08da378d20"></a>`additionalProperties`: `false`
- <a id="s-3354223e62"></a>`required`: `["actor","initiator","collection_id","collection_created_at","files_total","bytes_total","archive_root_sha256"]`
- <a id="s-9d44dee5db"></a>`title`: `"CollectionFinalizedData"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-10253e13a4"></a>`actor` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |
| <a id="s-943808cecb"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Archive Root Sha256" |  |
| <a id="s-0f4ed7f415"></a>`bytes_total` | yes | type="integer"; minimum=0; title="Bytes Total" |  |
| <a id="s-072c25f017"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Collection Created At" |  |
| <a id="s-b9b9559b23"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-634b16fda5"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; title="Context" |  |
| <a id="s-1b21fd8bfc"></a>`files_total` | yes | type="integer"; minimum=0; title="Files Total" |  |
| <a id="s-881387cb45"></a>`initiator` | yes | [RiverhogActor](schemas-riverhogactor.md) |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-3361a1cab5"></a>[field context · object value](#s-634b16fda5) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field archive_root_sha256](#s-943808cecb) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field collection_created_at](#s-072c25f017) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| [field context · object value](#s-3361a1cab5) | `encoded-size · bytes · contract_max` | maximum=4096; reason="bounded-lifecycle-event-context"; source_constraint={"field":"x-riverhog-encoded-bytes-max"} |

## Maintained corroboration

### Referenced contract elements

- [CollectionId](schemas-collectionid.md)
- [RiverhogActor](schemas-riverhogactor.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-89f71cf4b3"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-3d4b288705"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-2fb68f0bab"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L349)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionFinalizedData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e70fdad721ac682d7bf9cd1f3751c219236e25692bdec1b7e67122236ae888d2 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actor": {
      "$ref": "#/components/schemas/RiverhogActor"
    },
    "archive_root_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Archive Root Sha256",
      "type": "string"
    },
    "bytes_total": {
      "minimum": 0,
      "title": "Bytes Total",
      "type": "integer"
    },
    "collection_created_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Collection Created At",
      "type": "string"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "context": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object",
          "x-riverhog-encoded-bytes-max": 4096,
          "x-riverhog-extent": {
            "policy": "contract_max",
            "reason": "bounded-lifecycle-event-context"
          }
        },
        {
          "type": "null"
        }
      ],
      "title": "Context"
    },
    "files_total": {
      "minimum": 0,
      "title": "Files Total",
      "type": "integer"
    },
    "initiator": {
      "$ref": "#/components/schemas/RiverhogActor"
    }
  },
  "required": [
    "actor",
    "initiator",
    "collection_id",
    "collection_created_at",
    "files_total",
    "bytes_total",
    "archive_root_sha256"
  ],
  "title": "CollectionFinalizedData",
  "type": "object"
}
```

</details>
