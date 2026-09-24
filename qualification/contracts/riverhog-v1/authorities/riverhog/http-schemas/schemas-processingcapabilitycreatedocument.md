# schemas: ProcessingCapabilityCreateDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-processingcapabilitycreatedocument:04d4b95630 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-4bcfa552e1"></a>

- <a id="s-642207cbe8"></a>`type`: `"object"`
- <a id="s-bab973a6a5"></a>`additionalProperties`: `false`
- <a id="s-b8f61830ec"></a>`required`: `["fence","audience"]`
- <a id="s-ece357c726"></a>`title`: `"ProcessingCapabilityCreateDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-849b088487"></a>`actions` | no | type="array"; items=(type="string"; enum=["read-inputs","write-output"]); minItems=1; oneOf=[(const=["read-inputs"]); (const=["read-inputs","write-output"])]; title="Actions" |  |
| <a id="s-70af1dcf71"></a>`audience` | yes | type="string"; pattern="^[a-z0-9][a-z0-9._:/-]{0,299}$"; title="Audience" |  |
| <a id="s-19e40c46f5"></a>`fence` | yes | [NonnegativeDecimal](schemas-nonnegativedecimal.md); ge=1 |  |
| <a id="s-a3975d5305"></a>`ttl_seconds` | no | type="integer"; minimum=30; maximum=86400; default=900; title="Ttl Seconds" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field actions](#s-849b088487) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=86400; minimum=30; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field ttl_seconds](#s-a3975d5305) | `value · schema-value · contract_max` | shared above |

## Maintained corroboration

### Referenced contract elements

- [NonnegativeDecimal](schemas-nonnegativedecimal.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3e9f992b9c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-a66db0f219"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-92f2fb3b62"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ProcessingCapabilityCreateDocument`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4597d16e206531295aa8fd943d626acb22d4da89de2ea54dec416fb1f86df063 -->

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
    "audience": {
      "pattern": "^[a-z0-9][a-z0-9._:/-]{0,299}$",
      "title": "Audience",
      "type": "string"
    },
    "fence": {
      "$ref": "#/components/schemas/NonnegativeDecimal",
      "ge": 1
    },
    "ttl_seconds": {
      "default": 900,
      "maximum": 86400,
      "minimum": 30,
      "title": "Ttl Seconds",
      "type": "integer"
    }
  },
  "required": [
    "fence",
    "audience"
  ],
  "title": "ProcessingCapabilityCreateDocument",
  "type": "object"
}
```

</details>
