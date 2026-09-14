# stove0_observer_support.ObserverConformanceResult

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerconformanceresult:82b6a495cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc4e720fe4"></a>
- <a id="s-a04371e892"></a>`distribution`: `stove0-observer-support`
- <a id="s-0b2a786915"></a>`module`: `stove0_observer_support`
- <a id="s-ef0f9faad0"></a>`name`: `ObserverConformanceResult`
- <a id="s-268fd9a495"></a>`unit`: `export`

### Declared structure

- <a id="s-8f0566d785"></a>`kind`: `"class"`
- <a id="s-0992fdf9a7"></a>`signature`: `"\"(*, format: Literal['stove0-observer-conformance-result/v1'] = 'stove0-observer-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], descriptor: stove0_protocol.models.ObserverDescriptor, coverage: stove0_observer_support.conformance.ObserverConformanceCoverage, contracts: tuple[stove0_observer_support.conformance.ObserverContractConformance, ...]) -> None\""`

#### Validated model schema

<a id="s-837129e687"></a>
- <a id="s-9aabec6c04"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e8467985db"></a>`contracts` | yes | type="array"; items=(#/$defs/ObserverContractConformance) |  |
| <a id="s-4239338330"></a>`coverage` | yes | #/$defs/ObserverConformanceCoverage |  |
| <a id="s-f6874849e5"></a>`descriptor` | yes | #/$defs/ObserverDescriptor |  |
| <a id="s-3796d93708"></a>`format` | no | type="string"; const="stove0-observer-conformance-result/v1" |  |
| <a id="s-07cec605ce"></a>`status` | yes | type="string"; enum=["conformant","partially-exercised","inspected"] |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-3ce92b5f73"></a>`ArtifactSubject` | type="object"; fields=`bytes`, `collection`, `id`, `media_type`, `path`, `role`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-d02ffcac30"></a>`CollectionId` | type="integer"; minimum=1 |
| <a id="s-fe9cd9564b"></a>`CollectionRootRef` | type="object"; fields=`archive_root_sha256`, `collection_id`, `content_identity`; additional keys=`additionalProperties`, `required` |
| <a id="s-fea7121035"></a>`JsonSchemaDocument` | type="object"; fields=`dialect`, `format_policy`, `id`, `schema`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-5225835bde"></a>`JsonValue` | empty object |
| <a id="s-e4667b1362"></a>`ObservationFailure` | type="object"; fields=`code`, `message`, `retryable`; additional keys=`additionalProperties`, `required` |
| <a id="s-ed03bdd4d4"></a>`ObservationInapplicable` | type="object"; fields=`code`, `message`; additional keys=`additionalProperties`, `required` |
| <a id="s-5a67684e80"></a>`ObservationRequest` | type="object"; fields=`format`, `maximum_result_bytes`, `observer_contract_id`, `observer_contract_sha256`, `observer_descriptor_sha256`, `observer_registration_id`, `options`, `request_id`, `retrieval_policy`, `subjects`, `timeout_seconds`, `work_id`; additional keys=`additionalProperties`, `required` |
| <a id="s-72b70baceb"></a>`ObservationResult` | type="object"; fields=`execution_evidence`, `facts`, `facts_schema`, `facts_sha256`, `failure`, `format`, `inapplicable`, `observer`, `observer_contract_id`, `observer_contract_sha256`, `request_id`, `result_sha256`, `state`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-ed0b4a623e"></a>`ObserverConformanceCoverage` | type="object"; fields=`advertised`, `complete`, `exercised`; additional keys=`additionalProperties`, `required` |
| <a id="s-1810706536"></a>`ObserverContractConformance` | type="object"; fields=`contract_id`, `contract_sha256`, `evidence`, `execution`, `facts_schema_sha256`, `facts_semantics_conformance_vectors_sha256`, `facts_semantics_id`, `facts_semantics_sha256`, `maximum_result_bytes`, `options_schema_sha256`, `preferred_subject_batch_size`, `semantic_acceptance`; additional keys=`additionalProperties`, `required` |
| <a id="s-e9d1642adb"></a>`ObserverContractConformanceEvidence` | type="object"; fields=`observation`, `request`; additional keys=`additionalProperties`, `required` |
| <a id="s-e95a44f82c"></a>`ObserverContractSupport` | type="object"; fields=`contract_id`, `contract_sha256`, `facts_schema`, `facts_semantics`, `maximum_result_bytes`, `options_schema`, `preferred_subject_batch_size`; additional keys=`additionalProperties`, `required` |
| <a id="s-21208e2a60"></a>`ObserverDescriptor` | type="object"; fields=`contracts`, `descriptor_sha256`, `image_digest`, `implementation_id`, `implementation_version`, `protocol`, `source_revision`; additional keys=`additionalProperties`, `required` |
| <a id="s-9addfdd17e"></a>`ObserverImplementation` | type="object"; fields=`descriptor_sha256`, `id`, `protocol`, `source_revision`, `version`; additional keys=`additionalProperties`, `required` |
| <a id="s-4af9735373"></a>`ObserverSemanticAcceptance` | type="object"; fields=`kind`, `profile_id`, `profile_sha256`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-e03614fd2e"></a>`ObserverSemanticVectorEvidence` | type="object"; fields=`accepted_vector_ids`, `rejected_vector_ids`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-e3a938c4ae"></a>`SemanticFactsConformanceVector` | type="object"; fields=`accepted`, `facts`, `id`, `options`, `subjects`; additional keys=`additionalProperties`, `required` |
| <a id="s-54f57b5506"></a>`SemanticFactsConformanceVectors` | type="object"; fields=`format`, `profile_id`, `vectors`; additional keys=`additionalProperties`, `required` |
| <a id="s-94f2656e94"></a>`SemanticValidationProfile` | type="object"; fields=`conformance_vectors_sha256`, `id`, `profile_sha256`, `rules`; additional keys=`additionalProperties`, `required` |

## Maintained corroboration

### Related interface records

- [validate_result](stove0-observer-support-observerconformanceresult-validate-result.md)

## Governing policies

- <a id="pa-545f5f77df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverConformanceResult`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ab15e01833e8ccefb499417f21bf15f9129d3da53e2072831586adf57341980 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArtifactSubject": {
          "additionalProperties": false,
          "properties": {
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection": {
              "$ref": "#/$defs/CollectionRootRef"
            },
            "id": {
              "pattern": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$",
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
              "default": null
            },
            "path": {
              "maxLength": 4096,
              "minLength": 1,
              "type": "string"
            },
            "role": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "content_identity": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "collection_id",
            "archive_root_sha256",
            "content_identity"
          ],
          "type": "object"
        },
        "JsonSchemaDocument": {
          "additionalProperties": false,
          "properties": {
            "dialect": {
              "const": "https://json-schema.org/draft/2020-12/schema",
              "default": "https://json-schema.org/draft/2020-12/schema",
              "type": "string"
            },
            "format_policy": {
              "const": "annotation-only",
              "default": "annotation-only",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "schema": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            }
          },
          "required": [
            "id",
            "sha256",
            "schema"
          ],
          "type": "object"
        },
        "JsonValue": {},
        "ObservationFailure": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "retryable": {
              "type": "boolean"
            }
          },
          "required": [
            "code",
            "message",
            "retryable"
          ],
          "type": "object"
        },
        "ObservationInapplicable": {
          "additionalProperties": false,
          "properties": {
            "code": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "message": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "code",
            "message"
          ],
          "type": "object"
        },
        "ObservationRequest": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-observation-request/v1",
              "default": "stove0-observation-request/v1",
              "type": "string"
            },
            "maximum_result_bytes": {
              "default": 1048576,
              "maximum": 67108864,
              "minimum": 1,
              "type": "integer"
            },
            "observer_contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "observer_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "observer_descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "observer_registration_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$",
              "type": "string"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "request_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "retrieval_policy": {
              "default": "available-only",
              "enum": [
                "available-only",
                "allow"
              ],
              "type": "string"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "type": "array"
            },
            "timeout_seconds": {
              "default": 300,
              "maximum": 86400,
              "minimum": 1,
              "type": "integer"
            },
            "work_id": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        },
        "ObservationResult": {
          "additionalProperties": false,
          "properties": {
            "execution_evidence": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
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
              "default": null
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
              "default": null
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
              "type": "string"
            },
            "observer_contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "request_id": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "result_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "state": {
              "enum": [
                "observed",
                "inapplicable",
                "failed",
                "canceled"
              ],
              "type": "string"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
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
          "type": "object"
        },
        "ObserverConformanceCoverage": {
          "additionalProperties": false,
          "properties": {
            "advertised": {
              "minimum": 0,
              "type": "integer"
            },
            "complete": {
              "type": "boolean"
            },
            "exercised": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "advertised",
            "exercised",
            "complete"
          ],
          "type": "object"
        },
        "ObserverContractConformance": {
          "additionalProperties": false,
          "properties": {
            "contract_id": {
              "type": "string"
            },
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "type": "string"
            },
            "facts_schema_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "default": null
            },
            "facts_semantics_id": {
              "type": "string"
            },
            "facts_semantics_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "maximum_result_bytes": {
              "minimum": 1,
              "type": "integer"
            },
            "options_schema_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "preferred_subject_batch_size": {
              "minimum": 1,
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
          "type": "object"
        },
        "ObserverContractSupport": {
          "additionalProperties": false,
          "properties": {
            "contract_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "contract_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
              "type": "integer"
            },
            "options_schema": {
              "$ref": "#/$defs/JsonSchemaDocument"
            },
            "preferred_subject_batch_size": {
              "default": 128,
              "minimum": 1,
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
              "type": "array"
            },
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "image_digest": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "implementation_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "implementation_version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            },
            "protocol": {
              "const": "stove0-content-observer/v1",
              "default": "stove0-content-observer/v1",
              "type": "string"
            },
            "source_revision": {
              "maxLength": 200,
              "minLength": 1,
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
          "type": "object"
        },
        "ObserverImplementation": {
          "additionalProperties": false,
          "properties": {
            "descriptor_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "protocol": {
              "const": "stove0-content-observer/v1",
              "default": "stove0-content-observer/v1",
              "type": "string"
            },
            "source_revision": {
              "maxLength": 200,
              "minLength": 1,
              "type": "string"
            },
            "version": {
              "maxLength": 120,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "id",
            "version",
            "source_revision",
            "descriptor_sha256"
          ],
          "type": "object"
        },
        "ObserverSemanticAcceptance": {
          "additionalProperties": false,
          "properties": {
            "kind": {
              "enum": [
                "schema-only-revalidated",
                "conformance-runner-attestation"
              ],
              "type": "string"
            },
            "profile_id": {
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
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
          "type": "object"
        },
        "ObserverSemanticVectorEvidence": {
          "additionalProperties": false,
          "properties": {
            "accepted_vector_ids": {
              "items": {
                "type": "string"
              },
              "type": "array"
            },
            "rejected_vector_ids": {
              "items": {
                "type": "string"
              },
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
          "type": "object"
        },
        "SemanticFactsConformanceVector": {
          "additionalProperties": false,
          "properties": {
            "accepted": {
              "type": "boolean"
            },
            "facts": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "options": {
              "additionalProperties": {
                "$ref": "#/$defs/JsonValue"
              },
              "type": "object"
            },
            "subjects": {
              "items": {
                "$ref": "#/$defs/ArtifactSubject"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "accepted",
            "subjects",
            "facts"
          ],
          "type": "object"
        },
        "SemanticFactsConformanceVectors": {
          "additionalProperties": false,
          "properties": {
            "format": {
              "const": "stove0-semantic-facts-conformance/v1",
              "default": "stove0-semantic-facts-conformance/v1",
              "type": "string"
            },
            "profile_id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "vectors": {
              "items": {
                "$ref": "#/$defs/SemanticFactsConformanceVector"
              },
              "minItems": 2,
              "type": "array"
            }
          },
          "required": [
            "profile_id",
            "vectors"
          ],
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
              "default": null
            },
            "id": {
              "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
              "type": "string"
            },
            "profile_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "rules": {
              "items": {
                "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
                "type": "string"
              },
              "minItems": 1,
              "type": "array"
            }
          },
          "required": [
            "id",
            "rules",
            "profile_sha256"
          ],
          "type": "object"
        }
      },
      "additionalProperties": false,
      "properties": {
        "contracts": {
          "items": {
            "$ref": "#/$defs/ObserverContractConformance"
          },
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
          "type": "string"
        },
        "status": {
          "enum": [
            "conformant",
            "partially-exercised",
            "inspected"
          ],
          "type": "string"
        }
      },
      "required": [
        "status",
        "descriptor",
        "coverage",
        "contracts"
      ],
      "type": "object"
    },
    "signature": "\"(*, format: Literal['stove0-observer-conformance-result/v1'] = 'stove0-observer-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], descriptor: stove0_protocol.models.ObserverDescriptor, coverage: stove0_observer_support.conformance.ObserverConformanceCoverage, contracts: tuple[stove0_observer_support.conformance.ObserverContractConformance, ...]) -> None\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObserverConformanceResult",
  "unit": "export"
}
```
