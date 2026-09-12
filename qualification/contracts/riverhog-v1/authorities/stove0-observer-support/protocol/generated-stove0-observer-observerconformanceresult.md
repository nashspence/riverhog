# generated:stove0-observer: ObserverConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:stove0-observer-support:generated-stove0-observer-observerconformanceresult:bab9f16cd8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-d47202b295e8) |
| Contract elements | 1 |
| Extent decisions | 30 |

## External contract

<a id="s-fc5a6c3beff0"></a>
- <a id="s-c607c4203bb5"></a>`title`: ObserverConformanceResult
- <a id="s-79c5c6f59403"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-11cb6cb7b0ee"></a>`contracts` | yes | type="array"; items=(#/$defs/ObserverContractConformance) |  |
| <a id="s-6e150e7a08f9"></a>`coverage` | yes | #/$defs/ObserverConformanceCoverage |  |
| <a id="s-503986d75d58"></a>`descriptor` | yes | #/$defs/ObserverDescriptor |  |
| <a id="s-f07392d54143"></a>`format` | no | type="string"; const="stove0-observer-conformance-result/v1" |  |
| <a id="s-86e322f70a12"></a>`status` | yes | type="string"; enum=["conformant","partially-exercised","inspected"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-6da8dc866c3a"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-31ea8cf25bfd"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-44b34c0b44ab"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-1bf0987e08ce"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-ff4601229e60"></a>`JsonValue` | empty object |
| <a id="s-50f0629b745f"></a>`ObservationFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-93a287d09a41"></a>`ObservationInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-257aa2bfb3fd"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-c17307bef80c"></a>`ObservationResult` | type="object"; fields=`execution_evidence`, `facts`, `facts_schema`, `facts_sha256`, `failure`, `format`, `inapplicable`, `observer`, `observer_contract_id`, `observer_contract_sha256`, `request_id`, `result_sha256`, `state`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-a0b35db38ebf"></a>`ObserverConformanceCoverage` | type="object"; fields=`advertised`, `complete`, `exercised`; additional keys=`additionalProperties`, `required` |
| <a id="s-3429ae0b0c4d"></a>`ObserverContractConformance` | type="object"; fields=`contract_id`, `contract_sha256`, `evidence`, `execution`, `facts_schema_sha256`, `facts_semantics_conformance_vectors_sha256`, `facts_semantics_id`, `facts_semantics_sha256`, `maximum_result_bytes`, `options_schema_sha256`, `preferred_subject_batch_size`, `semantic_acceptance`; additional keys=`additionalProperties`, `required` |
| <a id="s-6014cd696059"></a>`ObserverContractConformanceEvidence` | type="object"; fields=`observation`, `request`; additional keys=`additionalProperties`, `required` |
| <a id="s-c7de29acfc02"></a>`ObserverContractSupport` | type="object"; fields=`contract_id`, `contract_sha256`, `facts_schema`, `facts_semantics`, `maximum_result_bytes`, `options_schema`, `preferred_subject_batch_size`; additional keys=`additionalProperties`, `required` |
| <a id="s-c720233b4b14"></a>`ObserverDescriptor` | type="object"; fields=`contracts`, `descriptor_sha256`, `image_digest`, `implementation_id`, `implementation_version`, `protocol`, `source_revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-ff3c4a891ae4"></a>`ObserverImplementation` | type="object"; fields=`descriptor_sha256`, `id`, `protocol`, `source_revision`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-cb936e6e5e7a"></a>`ObserverSemanticAcceptance` | type="object"; fields=`kind`, `profile_id`, `profile_sha256`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-cadb8352a7c3"></a>`ObserverSemanticVectorEvidence` | type="object"; fields=`accepted_vector_ids`, `rejected_vector_ids`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-c3aa3f50511f"></a>`SemanticFactsConformanceVector` | type="object"; fields=`accepted`, `facts`, `id`, `options`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-02d07e8151c9"></a>`SemanticFactsConformanceVectors` | type="object"; fields=`format`, `profile_id`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-5fcce07d86c8"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"stove0-observer-protocol"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field contracts](#s-11cb6cb7b0ee) | `cardinality · items · operational_policy` | shared above |
| <a id="s-18fec8dde8bd"></a>definition ObservationResult · field execution_evidence | `cardinality · entries · operational_policy` | shared above |
| <a id="s-454b88910233"></a>definition ObservationResult · field facts · anyOf alternative 1 | `cardinality · entries · operational_policy` | shared above |
| <a id="s-f5c3f9cb963c"></a>definition ObservationResult · field subjects | `cardinality · items · operational_policy` | shared above |
| <a id="s-2ef1f8a48742"></a>definition ObserverDescriptor · field contracts | `cardinality · items · operational_policy` | shared above |
| <a id="s-84a0c0984623"></a>definition ObserverSemanticVectorEvidence · field accepted_vector_ids | `cardinality · items · operational_policy` | shared above |
| <a id="s-a7a7a2ea7ce8"></a>definition ObserverSemanticVectorEvidence · field rejected_vector_ids | `cardinality · items · operational_policy` | shared above |
| <a id="s-b56714fb4e37"></a>definition SemanticFactsConformanceVector · field facts | `cardinality · entries · operational_policy` | shared above |
| <a id="s-14bd5c07e0af"></a>definition SemanticFactsConformanceVector · field options | `cardinality · entries · operational_policy` | shared above |
| <a id="s-a0524302e682"></a>definition SemanticFactsConformanceVector · field subjects | `cardinality · items · operational_policy` | shared above |
| <a id="s-34b15007e4e2"></a>definition SemanticFactsConformanceVectors · field vectors | `cardinality · items · operational_policy` | shared above |
| <a id="s-90fed6464fce"></a>definition SemanticValidationProfile · field rules | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-689a9a00f00d"></a>definition ObservationResult · field facts_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0cc5d9dcbc43"></a>definition ObservationResult · field observer_contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-8e01b8f9e559"></a>definition ObservationResult · field request_id | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0d1cf88363cd"></a>definition ObservationResult · field result_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-61309d58efac"></a>definition ObserverContractConformance · field contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-38d741705101"></a>definition ObserverContractConformance · field facts_schema_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-24b459cc3296"></a>definition ObserverContractConformance · field facts_semantics_conformance_vectors_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-4d9c57cb7ae5"></a>definition ObserverContractConformance · field facts_semantics_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f25ed6d63b3d"></a>definition ObserverContractConformance · field options_schema_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0bf8abada42b"></a>definition ObserverContractSupport · field contract_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-6c87e3c09e12"></a>definition ObserverContractSupport · field maximum_result_bytes | `value · schema-value · contract_max` | maximum=67108864; minimum=1; reason="schema-maximum" |
| <a id="s-42cfeceeffcb"></a>definition ObserverDescriptor · field descriptor_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-f9bf8ce9e8a9"></a>definition ObserverDescriptor · field image_digest | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-dceba6dd812f"></a>definition ObserverDescriptor · field implementation_version | `length · characters · contract_max` | maximum=120; minimum=1; reason="schema-maximum" |
| <a id="s-065ca78d50a8"></a>definition ObserverDescriptor · field source_revision | `length · characters · contract_max` | maximum=200; minimum=1; reason="schema-maximum" |
| <a id="s-4696fbe59acf"></a>definition ObserverSemanticAcceptance · field profile_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-2852262eab73"></a>definition SemanticValidationProfile · field conformance_vectors_sha256 · anyOf alternative 1 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-45433a22b149"></a>definition SemanticValidationProfile · field profile_sha256 | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-5a18b69d6833"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-e4c6b3bdefd5"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-eb6dc04fe458"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:stove0-observer](../../../evidence/sources.md#src-dcc0b5485b73) — `reference/stove0/packages/observer-support/src/stove0_observer_support/schemas.py::observer_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:stove0-observer/schemas/ObserverConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e27ed5abfaeb7544a3910aa4ab0216c2fc7f5fe0341296cb0b299304e601db95 -->

```json
{
  "$defs": {
    "ArtifactSubject": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "title": "Bytes",
          "type": "integer"
        },
        "collection": {
          "$ref": "#/$defs/CollectionRootRef"
        },
        "id": {
          "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "media_type": {
          "anyOf": [
            {
              "maxLength": 255,
              "minLength": 1,
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Media Type"
        },
        "path": {
          "maxLength": 4096,
          "minLength": 1,
          "title": "Path",
          "type": "string"
        },
        "role": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Role",
          "type": "string"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "role",
        "collection",
        "path",
        "bytes",
        "sha256"
      ],
      "title": "ArtifactSubject",
      "type": "object"
    },
    "CollectionId": {
      "minimum": 1,
      "type": "integer"
    },
    "CollectionRootRef": {
      "additionalProperties": false,
      "properties": {
        "archive_root_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Archive Root Sha256",
          "type": "string"
        },
        "collection_id": {
          "$ref": "#/$defs/CollectionId"
        },
        "content_identity": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Content Identity",
          "type": "string"
        }
      },
      "required": [
        "collection_id",
        "archive_root_sha256",
        "content_identity"
      ],
      "title": "CollectionRootRef",
      "type": "object"
    },
    "JsonSchemaDocument": {
      "additionalProperties": false,
      "properties": {
        "dialect": {
          "const": "https://json-schema.org/draft/2020-12/schema",
          "default": "https://json-schema.org/draft/2020-12/schema",
          "title": "Dialect",
          "type": "string"
        },
        "format_policy": {
          "const": "annotation-only",
          "default": "annotation-only",
          "title": "Format Policy",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Schema",
          "type": "object"
        },
        "sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Sha256",
          "type": "string"
        }
      },
      "required": [
        "id",
        "sha256",
        "schema"
      ],
      "title": "JsonSchemaDocument",
      "type": "object"
    },
    "JsonValue": {},
    "ObservationFailure": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        },
        "retryable": {
          "title": "Retryable",
          "type": "boolean"
        }
      },
      "required": [
        "code",
        "message",
        "retryable"
      ],
      "title": "ObservationFailure",
      "type": "object"
    },
    "ObservationInapplicable": {
      "additionalProperties": false,
      "properties": {
        "code": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Code",
          "type": "string"
        },
        "message": {
          "maxLength": 1000,
          "minLength": 1,
          "title": "Message",
          "type": "string"
        }
      },
      "required": [
        "code",
        "message"
      ],
      "title": "ObservationInapplicable",
      "type": "object"
    },
    "ObservationRequest": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-observation-request/v1",
          "default": "stove0-observation-request/v1",
          "title": "Format",
          "type": "string"
        },
        "maximum_result_bytes": {
          "default": 1048576,
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Observer Contract Id",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Contract Sha256",
          "type": "string"
        },
        "observer_descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Descriptor Sha256",
          "type": "string"
        },
        "observer_registration_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
          "title": "Observer Registration Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
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
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        },
        "timeout_seconds": {
          "default": 300,
          "maximum": 86400,
          "minimum": 1,
          "title": "Timeout Seconds",
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Work Id",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "observer_registration_id",
        "observer_descriptor_sha256",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects",
        "request_id"
      ],
      "title": "ObservationRequest",
      "type": "object"
    },
    "ObservationResult": {
      "additionalProperties": false,
      "properties": {
        "execution_evidence": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Execution Evidence",
          "type": "object"
        },
        "facts": {
          "anyOf": [
            {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Facts"
        },
        "facts_schema": {
          "anyOf": [
            {
              "$ref": "#/$defs/JsonSchemaDocument"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "facts_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Facts Sha256"
        },
        "failure": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObservationFailure"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "format": {
          "const": "stove0-observation-result/v1",
          "default": "stove0-observation-result/v1",
          "title": "Format",
          "type": "string"
        },
        "inapplicable": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObservationInapplicable"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "observer": {
          "$ref": "#/$defs/ObserverImplementation"
        },
        "observer_contract_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Observer Contract Id",
          "type": "string"
        },
        "observer_contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Observer Contract Sha256",
          "type": "string"
        },
        "request_id": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Request Id",
          "type": "string"
        },
        "result_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Result Sha256",
          "type": "string"
        },
        "state": {
          "enum": [
            "observed",
            "inapplicable",
            "failed",
            "canceled"
          ],
          "title": "State",
          "type": "string"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        }
      },
      "required": [
        "request_id",
        "state",
        "observer",
        "observer_contract_id",
        "observer_contract_sha256",
        "subjects",
        "result_sha256"
      ],
      "title": "ObservationResult",
      "type": "object"
    },
    "ObserverConformanceCoverage": {
      "additionalProperties": false,
      "properties": {
        "advertised": {
          "minimum": 0,
          "title": "Advertised",
          "type": "integer"
        },
        "complete": {
          "title": "Complete",
          "type": "boolean"
        },
        "exercised": {
          "minimum": 0,
          "title": "Exercised",
          "type": "integer"
        }
      },
      "required": [
        "advertised",
        "exercised",
        "complete"
      ],
      "title": "ObserverConformanceCoverage",
      "type": "object"
    },
    "ObserverContractConformance": {
      "additionalProperties": false,
      "properties": {
        "contract_id": {
          "title": "Contract Id",
          "type": "string"
        },
        "contract_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Contract Sha256",
          "type": "string"
        },
        "evidence": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObserverContractConformanceEvidence"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        },
        "execution": {
          "enum": [
            "not-exercised",
            "exercised"
          ],
          "title": "Execution",
          "type": "string"
        },
        "facts_schema_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Facts Schema Sha256",
          "type": "string"
        },
        "facts_semantics_conformance_vectors_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Facts Semantics Conformance Vectors Sha256"
        },
        "facts_semantics_id": {
          "title": "Facts Semantics Id",
          "type": "string"
        },
        "facts_semantics_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Facts Semantics Sha256",
          "type": "string"
        },
        "maximum_result_bytes": {
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options_schema_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Options Schema Sha256",
          "type": "string"
        },
        "preferred_subject_batch_size": {
          "minimum": 1,
          "title": "Preferred Subject Batch Size",
          "type": "integer"
        },
        "semantic_acceptance": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObserverSemanticAcceptance"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "contract_id",
        "contract_sha256",
        "options_schema_sha256",
        "facts_schema_sha256",
        "facts_semantics_id",
        "facts_semantics_sha256",
        "preferred_subject_batch_size",
        "maximum_result_bytes",
        "execution"
      ],
      "title": "ObserverContractConformance",
      "type": "object"
    },
    "ObserverContractConformanceEvidence": {
      "additionalProperties": false,
      "properties": {
        "observation": {
          "$ref": "#/$defs/ObservationResult"
        },
        "request": {
          "$ref": "#/$defs/ObservationRequest"
        }
      },
      "required": [
        "request",
        "observation"
      ],
      "title": "ObserverContractConformanceEvidence",
      "type": "object"
    },
    "ObserverContractSupport": {
      "additionalProperties": false,
      "properties": {
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
        "facts_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "facts_semantics": {
          "$ref": "#/$defs/SemanticValidationProfile"
        },
        "maximum_result_bytes": {
          "maximum": 67108864,
          "minimum": 1,
          "title": "Maximum Result Bytes",
          "type": "integer"
        },
        "options_schema": {
          "$ref": "#/$defs/JsonSchemaDocument"
        },
        "preferred_subject_batch_size": {
          "default": 128,
          "minimum": 1,
          "title": "Preferred Subject Batch Size",
          "type": "integer"
        }
      },
      "required": [
        "contract_id",
        "contract_sha256",
        "options_schema",
        "facts_schema",
        "facts_semantics",
        "maximum_result_bytes"
      ],
      "title": "ObserverContractSupport",
      "type": "object"
    },
    "ObserverDescriptor": {
      "additionalProperties": false,
      "properties": {
        "contracts": {
          "items": {
            "$ref": "#/$defs/ObserverContractSupport"
          },
          "minItems": 1,
          "title": "Contracts",
          "type": "array"
        },
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Image Digest",
          "type": "string"
        },
        "implementation_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Implementation Id",
          "type": "string"
        },
        "implementation_version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Implementation Version",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-content-observer/v1",
          "default": "stove0-content-observer/v1",
          "title": "Protocol",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        }
      },
      "required": [
        "implementation_id",
        "implementation_version",
        "source_revision",
        "image_digest",
        "contracts",
        "descriptor_sha256"
      ],
      "title": "ObserverDescriptor",
      "type": "object"
    },
    "ObserverImplementation": {
      "additionalProperties": false,
      "properties": {
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "protocol": {
          "const": "stove0-content-observer/v1",
          "default": "stove0-content-observer/v1",
          "title": "Protocol",
          "type": "string"
        },
        "source_revision": {
          "maxLength": 200,
          "minLength": 1,
          "title": "Source Revision",
          "type": "string"
        },
        "version": {
          "maxLength": 120,
          "minLength": 1,
          "title": "Version",
          "type": "string"
        }
      },
      "required": [
        "id",
        "version",
        "source_revision",
        "descriptor_sha256"
      ],
      "title": "ObserverImplementation",
      "type": "object"
    },
    "ObserverSemanticAcceptance": {
      "additionalProperties": false,
      "description": "Exact schema revalidation or explicit conformance-runner attestation.",
      "properties": {
        "kind": {
          "enum": [
            "schema-only-revalidated",
            "conformance-runner-attestation"
          ],
          "title": "Kind",
          "type": "string"
        },
        "profile_id": {
          "title": "Profile Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "vectors": {
          "anyOf": [
            {
              "$ref": "#/$defs/ObserverSemanticVectorEvidence"
            },
            {
              "type": "null"
            }
          ],
          "default": null
        }
      },
      "required": [
        "kind",
        "profile_id",
        "profile_sha256"
      ],
      "title": "ObserverSemanticAcceptance",
      "type": "object"
    },
    "ObserverSemanticVectorEvidence": {
      "additionalProperties": false,
      "properties": {
        "accepted_vector_ids": {
          "items": {
            "type": "string"
          },
          "title": "Accepted Vector Ids",
          "type": "array"
        },
        "rejected_vector_ids": {
          "items": {
            "type": "string"
          },
          "title": "Rejected Vector Ids",
          "type": "array"
        },
        "vectors": {
          "$ref": "#/$defs/SemanticFactsConformanceVectors"
        }
      },
      "required": [
        "vectors",
        "accepted_vector_ids",
        "rejected_vector_ids"
      ],
      "title": "ObserverSemanticVectorEvidence",
      "type": "object"
    },
    "SemanticFactsConformanceVector": {
      "additionalProperties": false,
      "properties": {
        "accepted": {
          "title": "Accepted",
          "type": "boolean"
        },
        "facts": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Facts",
          "type": "object"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "options": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "title": "Options",
          "type": "object"
        },
        "subjects": {
          "items": {
            "$ref": "#/$defs/ArtifactSubject"
          },
          "minItems": 1,
          "title": "Subjects",
          "type": "array"
        }
      },
      "required": [
        "id",
        "accepted",
        "subjects",
        "facts"
      ],
      "title": "SemanticFactsConformanceVector",
      "type": "object"
    },
    "SemanticFactsConformanceVectors": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "stove0-semantic-facts-conformance/v1",
          "default": "stove0-semantic-facts-conformance/v1",
          "title": "Format",
          "type": "string"
        },
        "profile_id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Profile Id",
          "type": "string"
        },
        "vectors": {
          "items": {
            "$ref": "#/$defs/SemanticFactsConformanceVector"
          },
          "minItems": 2,
          "title": "Vectors",
          "type": "array"
        }
      },
      "required": [
        "profile_id",
        "vectors"
      ],
      "title": "SemanticFactsConformanceVectors",
      "type": "object"
    },
    "SemanticValidationProfile": {
      "additionalProperties": false,
      "properties": {
        "conformance_vectors_sha256": {
          "anyOf": [
            {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "title": "Conformance Vectors Sha256"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Profile Sha256",
          "type": "string"
        },
        "rules": {
          "items": {
            "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
            "type": "string"
          },
          "minItems": 1,
          "title": "Rules",
          "type": "array"
        }
      },
      "required": [
        "id",
        "rules",
        "profile_sha256"
      ],
      "title": "SemanticValidationProfile",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "contracts": {
      "items": {
        "$ref": "#/$defs/ObserverContractConformance"
      },
      "title": "Contracts",
      "type": "array"
    },
    "coverage": {
      "$ref": "#/$defs/ObserverConformanceCoverage"
    },
    "descriptor": {
      "$ref": "#/$defs/ObserverDescriptor"
    },
    "format": {
      "const": "stove0-observer-conformance-result/v1",
      "default": "stove0-observer-conformance-result/v1",
      "title": "Format",
      "type": "string"
    },
    "status": {
      "enum": [
        "conformant",
        "partially-exercised",
        "inspected"
      ],
      "title": "Status",
      "type": "string"
    }
  },
  "required": [
    "status",
    "descriptor",
    "coverage",
    "contracts"
  ],
  "title": "ObserverConformanceResult",
  "type": "object"
}
```
