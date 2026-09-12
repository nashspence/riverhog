# schemas: ObserverUse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-observeruse:772a5a6a56 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-20174c0218"></a>
- <a id="s-467e721d49"></a>`title`: ObserverUse
- <a id="s-f4e5e64527"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5d6aaa430a"></a>`artifact_rules` | no | type="array"; items=(#/components/schemas/ArtifactRule) |  |
| <a id="s-81278b328f"></a>`contract_id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-cd6fb17a6b"></a>`contract_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-82b6002609"></a>`maximum_result_bytes` | no | type="integer"; minimum=1; maximum=67108864 |  |
| <a id="s-1443df63c8"></a>`options` | no | type="object"; additional keys=`additionalProperties` |  |
| <a id="s-401f013502"></a>`registration_id` | yes | type="string" |  |
| <a id="s-c8ef1bbb04"></a>`retrieval_policy` | no | type="string"; enum=["available-only","allow"] |  |
| <a id="s-daf8962421"></a>`timeout_seconds` | no | type="integer"; minimum=1; maximum=86400 |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifact_rules](#s-5d6aaa430a) | `cardinality · items · operational_policy` | shared above |
| [field options](#s-1443df63c8) | `cardinality · entries · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contract_sha256](#s-cd6fb17a6b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field maximum_result_bytes](#s-82b6002609) | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| [field timeout_seconds](#s-daf8962421) | `value · schema-value · contract_max` | maximum=86400; minimum=1; reason="schema-maximum" |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactRule](schemas-artifactrule.md)
- [schemas: JsonValue](schemas-jsonvalue.md)

## Governing policies

- <a id="pa-bf85164f16"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-3fe7b2fce3"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-944098fac8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:stove0](../../../evidence/sources.md#src-52e6e32124) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/ObserverUse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 980cd90a66ccdcdbd0d8a24a5f1ae06e4ecf6f268c9e89d2cfb1a5cc662c3b87 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifact_rules": {
      "default": [
        {
          "glob": "*",
          "role": "stove0.source/v1"
        }
      ],
      "items": {
        "$ref": "#/components/schemas/ArtifactRule"
      },
      "title": "Artifact Rules",
      "type": "array"
    },
    "contract_id": {
      "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
      "title": "Contract Id",
      "type": "string"
    },
    "contract_sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Contract Sha256",
      "type": "string"
    },
    "maximum_result_bytes": {
      "default": 1048576,
      "maximum": 67108864,
      "minimum": 1,
      "title": "Maximum Result Bytes",
      "type": "integer"
    },
    "options": {
      "additionalProperties": {
        "$ref": "#/components/schemas/JsonValue"
      },
      "title": "Options",
      "type": "object"
    },
    "registration_id": {
      "title": "Registration Id",
      "type": "string"
    },
    "retrieval_policy": {
      "default": "available-only",
      "enum": [
        "available-only",
        "allow"
      ],
      "title": "Retrieval Policy",
      "type": "string"
    },
    "timeout_seconds": {
      "default": 300,
      "maximum": 86400,
      "minimum": 1,
      "title": "Timeout Seconds",
      "type": "integer"
    }
  },
  "required": [
    "registration_id",
    "contract_id",
    "contract_sha256"
  ],
  "title": "ObserverUse",
  "type": "object"
}
```
