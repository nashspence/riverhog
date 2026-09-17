# schemas: TransformCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-transformcapabilitydocument:f2785cd8d5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8356e4f64b"></a>

- <a id="s-8e0b731489"></a>`type`: `"object"`
- <a id="s-cb3941b1b6"></a>`additionalProperties`: `false`
- <a id="s-611b58a52a"></a>`required`: `["format","id","claim_id","fence","audience","actions","state","principal_app","expires_at","artifacts","token"]`
- <a id="s-6e56e9ccbf"></a>`title`: `"TransformCapabilityDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6cf1b8ff58"></a>`actions` | yes | type="array"; items=(type="string"; enum=["read-inputs","write-output"]); minItems=1; oneOf=[(const=["read-inputs"]); (const=["read-inputs","write-output"])]; title="Actions" |  |
| <a id="s-82d9c315a0"></a>`artifacts` | yes | [ArtifactReceivingSetDocument](schemas-artifactreceivingsetdocument.md) |  |
| <a id="s-23e4e467a7"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$"; title="Audience" |  |
| <a id="s-efc30a4fa5"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-d17899e432"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1; title="Expires At" |  |
| <a id="s-504b09c50c"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |
| <a id="s-46d5a9e506"></a>`format` | yes | type="string"; const="riverhog-transform-capability/v1"; title="Format" |  |
| <a id="s-359c369b8d"></a>`id` | yes | type="string"; maxLength=160; minLength=1; title="Id" |  |
| <a id="s-932409e908"></a>`principal_app` | yes | type="string"; maxLength=300; minLength=1; title="Principal App" |  |
| <a id="s-2f565ff4bc"></a>`state` | yes | type="string"; enum=["receiving","active"]; title="State" |  |
| <a id="s-d59acea44c"></a>`token` | yes | type="string"; pattern="^rhc_[A-Za-z0-9_-]+$"; title="Token" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field actions](#s-6cf1b8ff58) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-efc30a4fa5) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field expires_at](#s-d17899e432) | `length · characters · contract_max` | maximum=64; minimum=1; reason="schema-maximum" |
| [field id](#s-359c369b8d) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field principal_app](#s-932409e908) | `length · characters · contract_max` | maximum=300; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [ArtifactReceivingSetDocument](schemas-artifactreceivingsetdocument.md)

## Governing policies

- <a id="pa-a1a95069be"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-c51f15c08d"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-1a9f5f1438"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/TransformCapabilityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17e910223e622a78026a55a52e0d8af93f39c5ed7c175b4459bc71c7dc707c83 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "actions": {
      "items": {
        "enum": [
          "read-inputs",
          "write-output"
        ],
        "type": "string"
      },
      "minItems": 1,
      "oneOf": [
        {
          "const": [
            "read-inputs"
          ]
        },
        {
          "const": [
            "read-inputs",
            "write-output"
          ]
        }
      ],
      "title": "Actions",
      "type": "array"
    },
    "artifacts": {
      "$ref": "#/components/schemas/ArtifactReceivingSetDocument"
    },
    "audience": {
      "pattern": "^[a-z0-9][a-z0-9._:/-]{0,299}$",
      "title": "Audience",
      "type": "string"
    },
    "claim_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Claim Id",
      "type": "string"
    },
    "expires_at": {
      "maxLength": 64,
      "minLength": 1,
      "title": "Expires At",
      "type": "string"
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "format": {
      "const": "riverhog-transform-capability/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "principal_app": {
      "maxLength": 300,
      "minLength": 1,
      "title": "Principal App",
      "type": "string"
    },
    "state": {
      "enum": [
        "receiving",
        "active"
      ],
      "title": "State",
      "type": "string"
    },
    "token": {
      "pattern": "^rhc_[A-Za-z0-9_-]+$",
      "title": "Token",
      "type": "string"
    }
  },
  "required": [
    "format",
    "id",
    "claim_id",
    "fence",
    "audience",
    "actions",
    "state",
    "principal_app",
    "expires_at",
    "artifacts",
    "token"
  ],
  "title": "TransformCapabilityDocument",
  "type": "object"
}
```

</details>
