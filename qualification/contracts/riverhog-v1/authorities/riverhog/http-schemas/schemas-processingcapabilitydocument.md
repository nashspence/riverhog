# schemas: ProcessingCapabilityDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingcapabilitydocument:ea2a91e303 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-969a27aced"></a>

- <a id="s-84b803b1ee"></a>`type`: `"object"`
- <a id="s-ee33ae7d67"></a>`additionalProperties`: `false`
- <a id="s-b0296fe1d1"></a>`required`: `["format","id","claim_id","fence","audience","actions","state","principal_id","expires_at","artifacts","token"]`
- <a id="s-c05fbaa0b6"></a>`title`: `"ProcessingCapabilityDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4a9bfedff6"></a>`actions` | yes | type="array"; items=(type="string"; enum=["read-inputs","write-output"]); minItems=1; oneOf=[(const=["read-inputs"]); (const=["read-inputs","write-output"])]; title="Actions" |  |
| <a id="s-dbfc9397ae"></a>`artifacts` | yes | [ArtifactReceivingSetDocument](schemas-artifactreceivingsetdocument.md) |  |
| <a id="s-0030de3986"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$"; title="Audience" |  |
| <a id="s-30bae1c5f9"></a>`claim_id` | yes | type="string"; pattern="^[0-9a-f]{64}$"; title="Claim Id" |  |
| <a id="s-389557c02d"></a>`expires_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Expires At" |  |
| <a id="s-b8159e00ee"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-01112b8876"></a>`format` | yes | type="string"; const="riverhog-processing-capability/v1"; title="Format" |  |
| <a id="s-457409baab"></a>`id` | yes | type="string"; maxLength=160; minLength=1; title="Id" |  |
| <a id="s-7138e92d1f"></a>`principal_id` | yes | [PrincipalId](schemas-principalid.md); maxLength=300 |  |
| <a id="s-168657fc42"></a>`state` | yes | type="string"; enum=["receiving","active"]; title="State" |  |
| <a id="s-479e28075d"></a>`token` | yes | type="string"; pattern="^rhc_[A-Za-z0-9_-]+$"; title="Token" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field actions](#s-4a9bfedff6) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field claim_id](#s-30bae1c5f9) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field expires_at](#s-389557c02d) | `length · characters · fixed` | maximum=30; minimum=30; reason="fixed-public-representation" |
| [field id](#s-457409baab) | `length · characters · contract_max` | maximum=160; minimum=1; reason="schema-maximum" |
| [field principal_id](#s-7138e92d1f) | `length · characters · contract_max` | maximum=300; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract elements

- [ArtifactReceivingSetDocument](schemas-artifactreceivingsetdocument.md)
- [NonnegativeDecimal](schemas-nonnegativedecimal.md)
- [PrincipalId](schemas-principalid.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-ba35750fdd"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-b8ad309bda"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-49b9e68136"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingCapabilityDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 20b420a9b3c09859e73cb1e117df1f666f02d1d3d34ca3fc6b364602ae25cfe2 -->

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
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Expires At",
      "type": "string"
    },
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "format": {
      "const": "riverhog-processing-capability/v1",
      "title": "Format",
      "type": "string"
    },
    "id": {
      "maxLength": 160,
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "principal_id": {
      "$ref": "#/components/schemas/PrincipalId",
      "maxLength": 300
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
    "principal_id",
    "expires_at",
    "artifacts",
    "token"
  ],
  "title": "ProcessingCapabilityDocument",
  "type": "object"
}
```

</details>
