# schemas: TargetJobStatus

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-targetjobstatus:5145d7a958 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-5b62e18de1"></a>
- <a id="s-1b9c3b496c"></a>`title`: TargetJobStatus
- <a id="s-c5af2d7527"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-606c664aea"></a>`attempt` | yes | type="integer"; minimum=1 |  |
| <a id="s-b09da142f6"></a>`derivation` | no | anyOf=type="object"; additional keys=`additionalProperties` \| type="null" |  |
| <a id="s-57ab7a6f5e"></a>`effect_receipt` | no | anyOf=#/components/schemas/ExternalEffectReceipt \| type="null" |  |
| <a id="s-c57da9e956"></a>`execution_evidence` | no | anyOf=#/components/schemas/TargetExecutionEvidence \| type="null" |  |
| <a id="s-f403678d57"></a>`failure` | no | anyOf=#/components/schemas/TargetFailure \| type="null" |  |
| <a id="s-c77d39f4fe"></a>`inapplicable` | no | anyOf=#/components/schemas/TargetInapplicable \| type="null" |  |
| <a id="s-ce6a90cdcf"></a>`job_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-34fa32b154"></a>`output_collection` | no | anyOf=#/components/schemas/OutputCollectionRef \| type="null" |  |
| <a id="s-4858da8e4d"></a>`plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b13de944b9"></a>`production` | no | anyOf=#/components/schemas/TargetProductionAuthority \| type="null" |  |
| <a id="s-a4fb3e445f"></a>`progress` | yes | #/components/schemas/TargetProgress |  |
| <a id="s-9c0c396bbc"></a>`protocol` | no | type="string"; enum=["stove0-transform-target/v1","stove0-effect-target/v1"] |  |
| <a id="s-8373216865"></a>`request_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-b4712e4aec"></a>`state` | yes | type="string"; enum=["queued","running","canceling","interrupted","inapplicable","succeeded","failed","canceled"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-61d3179556"></a>[field derivation · object value](#s-b09da142f6) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field job_id](#s-ce6a90cdcf) | `length · characters · fixed` | shared above |
| [field plan_sha256](#s-4858da8e4d) | `length · characters · fixed` | shared above |
| [field request_sha256](#s-8373216865) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ExternalEffectReceipt](schemas-externaleffectreceipt.md)
- [schemas: OutputCollectionRef](schemas-outputcollectionref.md)
- [schemas: TargetExecutionEvidence](schemas-targetexecutionevidence.md)
- [schemas: TargetFailure](schemas-targetfailure.md)
- [schemas: TargetInapplicable](schemas-targetinapplicable.md)
- [schemas: TargetProductionAuthority](schemas-targetproductionauthority.md)
- [schemas: TargetProgress](schemas-targetprogress.md)

## Governing policies

- <a id="pa-7056fc5dcd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-c85bbba24f"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-cb032dea37"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/TargetJobStatus`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 24d2a389a999b1cc5fbabff12afdfded3bdda88b0515f472b543541ce57f3087 -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "failure": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "state": {
            "const": "failed"
          }
        }
      },
      "then": {
        "properties": {
          "failure": {
            "type": "object"
          }
        },
        "required": [
          "failure"
        ]
      }
    }
  ],
  "properties": {
    "attempt": {
      "minimum": 1,
      "title": "Attempt",
      "type": "integer"
    },
    "derivation": {
      "anyOf": [
        {
          "additionalProperties": true,
          "type": "object"
        },
        {
          "type": "null"
        }
      ],
      "title": "Derivation"
    },
    "effect_receipt": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ExternalEffectReceipt"
        },
        {
          "type": "null"
        }
      ]
    },
    "execution_evidence": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetExecutionEvidence"
        },
        {
          "type": "null"
        }
      ]
    },
    "failure": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetFailure"
        },
        {
          "type": "null"
        }
      ]
    },
    "inapplicable": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetInapplicable"
        },
        {
          "type": "null"
        }
      ]
    },
    "job_id": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Job Id",
      "type": "string"
    },
    "output_collection": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/OutputCollectionRef"
        },
        {
          "type": "null"
        }
      ]
    },
    "plan_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Plan Sha256",
      "type": "string"
    },
    "production": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/TargetProductionAuthority"
        },
        {
          "type": "null"
        }
      ]
    },
    "progress": {
      "$ref": "#/components/schemas/TargetProgress"
    },
    "protocol": {
      "default": "stove0-transform-target/v1",
      "enum": [
        "stove0-transform-target/v1",
        "stove0-effect-target/v1"
      ],
      "title": "Protocol",
      "type": "string"
    },
    "request_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Request Sha256",
      "type": "string"
    },
    "state": {
      "enum": [
        "queued",
        "running",
        "canceling",
        "interrupted",
        "inapplicable",
        "succeeded",
        "failed",
        "canceled"
      ],
      "title": "State",
      "type": "string"
    }
  },
  "required": [
    "job_id",
    "state",
    "attempt",
    "request_sha256",
    "plan_sha256",
    "progress"
  ],
  "title": "TargetJobStatus",
  "type": "object"
}
```
