# stove0_target_protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol:1bf702e65c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-target-protocol` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `distribution` | "stove0-target-protocol" |
| `exports` | additional keys=`ARTIFACT_ID_PATTERN`, `AcceptedTargetJob`, `EFFECT_RECEIPT_FORMAT`, `EFFECT_TARGET_PROTOCOL`, `EffectPlan`, `EffectPlanPayload`, `ExternalEffectReceipt`, `ExternalEffectReceiptPayload`, `InputArtifact`, `InputArtifactContract`, `InputDisposition`, `InputDispositionDeclaration`, `JSON_SCHEMA_ONLY_SEMANTIC_PROFILE`, `OperationContract`, `OperationContractPayload`, `OutputArtifact`, `OutputArtifactContract`, `OutputArtifactRoleCount`, `OutputArtifactSetIdentity`, `OutputCollectionRef`, `OutputSourceEdge`, `SHA256_PATTERN`, `SemanticId`, `SemanticIntentConformanceVector`, `SemanticIntentConformanceVectors`, `SemanticValidationProfile`, `SemanticValidationProfilePayload`, `Sha256`, `TARGET_CALLBACK_HTTP_OPERATIONS`, `TARGET_HTTP_OPERATIONS`, `TARGET_INPUT_PAGE_MAX`, `TRANSFORM_TARGET_PROTOCOL`, `TargetCallbackAccess`, `TargetCallbackAcknowledgement`, `TargetContract`, `TargetContractPayload`, `TargetDeclaration`, `TargetExecutionEvidence`, `TargetFailure`, `TargetInapplicable`, `TargetInputAuthority`, `TargetInputPage`, `TargetInputRoleCount`, `TargetJobDeclaration`, `TargetJobRequest`, `TargetJobState`, `TargetJobStatus`, `TargetOperationSupport`, `TargetOutputBinding`, `TargetOutputBindingSetIdentity`, `TargetPlan`, `TargetPreflightRequest`, `TargetPreflightResponse`, `TargetProductionAuthority`, `TargetProductionAuthorityPayload`, `TargetProductionSealResponse`, `TargetProgress`, `TargetProtocol`, `TargetProtocolModel`, `TargetResultKind`, `TargetRuntimeAuthority`, `TargetSettlementAuthority`, `TargetSettlementAuthorityPayload`, `TransformPlan`, `TransformPlanPayload`, `WorkspaceAssurance`, `canonical_json_bytes`, `canonical_json_sha256`, `update_input_disposition_commitment`, `update_output_artifact_commitment`, `update_output_source_edge_commitment`, `update_target_output_binding_commitment`, `validate_declaration_against_operation`, `validate_preflight_response_against_request`, `validate_status_against_request` |
| `module` | "stove0_target_protocol" |

## Governing policies

- `compatibility/python-api/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:stove0-target-protocol` — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py::<module>`

### Machine authority

- `/external_contract/python/23`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad353acfdb4798f70e68126b8ffd0d1b41a581883a38f75d65cf145d6edbd157 -->

```json
{
  "distribution": "stove0-target-protocol",
  "exports": {
    "ARTIFACT_ID_PATTERN": {
      "kind": "constant",
      "value": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"
    },
    "AcceptedTargetJob": {
      "kind": "class",
      "members": {
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "d6edef74a94d6ff90c51b59d72f6c750c6e411f0d80816c3136379e41e828867",
      "signature": "(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "EFFECT_RECEIPT_FORMAT": {
      "kind": "constant",
      "value": "stove0-external-effect-receipt/v1"
    },
    "EFFECT_TARGET_PROTOCOL": {
      "kind": "constant",
      "value": "stove0-effect-target/v1"
    },
    "EffectPlan": {
      "kind": "class",
      "members": {
        "binding_document": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, JsonValue]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'EffectPlanPayload') -> 'EffectPlan'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "017da00dc3a677999f1c2a61f4eebffe278e4b2c76f094adda813cdb2a72b220",
      "signature": "(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-effect-target/v1'] = 'stove0-effect-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = (), plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "EffectPlanPayload": {
      "kind": "class",
      "members": {
        "canonical_observation_results": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        }
      },
      "schema_sha256": "48b5632c032d7f4db1bf0ef5bb07926abc843434dca8cf052526b161b3157b3c",
      "signature": "(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-effect-target/v1'] = 'stove0-effect-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = ()) -> None"
    },
    "ExternalEffectReceipt": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'ExternalEffectReceiptPayload') -> 'ExternalEffectReceipt'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "d35b7a54eb7db5847253bd3315189fea8ca2a702b3516d1e794dca0d3c880621",
      "signature": "(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue], receipt_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ExternalEffectReceiptPayload": {
      "kind": "class",
      "members": {
        "bounded_result": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "d3489bc5c57692d8e98b5c61032d8c138fa5e476ccad3c14201bc96374d42f10",
      "signature": "(*, format: Literal['stove0-external-effect-receipt/v1'] = 'stove0-external-effect-receipt/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result: dict[str, JsonValue]) -> None"
    },
    "InputArtifact": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "6bae5bc6fdacfc380db9fd5107b64cf18ed580f1888a45a1b1a6e37731e60168",
      "signature": "(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_protocol.models.CollectionRootRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None"
    },
    "InputArtifactContract": {
      "kind": "class",
      "members": {
        "canonical_dispositions": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[InputDisposition, ...] | None') -> 'tuple[InputDisposition, ...] | None'"
        },
        "validate_cardinality": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "a77940dd0cdd6154e96246459b292378e83a95b94bd488f79c5ead938fb2a73c",
      "signature": "(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int | None, Ge(ge=1)] = None, allowed_dispositions: tuple[typing.Literal['transformed', 'preserved', 'omitted', 'rejected'], ...] | None = None) -> None"
    },
    "InputDisposition": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "InputDispositionDeclaration": {
      "kind": "class",
      "schema_sha256": "e94d6588a913858886bdbd76c5ea8b089aeca0bc91025c491822f3a23edf6dba",
      "signature": "(*, input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], status: Literal['transformed', 'preserved', 'omitted', 'rejected']) -> None"
    },
    "JSON_SCHEMA_ONLY_SEMANTIC_PROFILE": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "OperationContract": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'OperationContractPayload') -> 'OperationContract'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "aa2a5d676e27f1328d9ac0508bb514c28355ff594b7e72c052731d4fbc4edd22",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument | None = None, source_retirement_permitted: bool = False, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "OperationContractPayload": {
      "kind": "class",
      "members": {
        "bind_semantic_conformance_vectors": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "validate_roles": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "81f42d05e99391558a8842214e784f88daef258d34fe0579d6cdb9ca136913db",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', intent_schema: stove0_protocol.models.JsonSchemaDocument, intent_semantics: stove0_protocol.models.SemanticValidationProfile, inputs: Annotated[tuple[stove0_target_protocol.protocol.InputArtifactContract, ...], MinLen(min_length=1)], outputs: tuple[stove0_target_protocol.protocol.OutputArtifactContract, ...] = (), effect_receipt_schema: stove0_protocol.models.JsonSchemaDocument | None = None, source_retirement_permitted: bool = False) -> None"
    },
    "OutputArtifact": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "199abbe3d7d9a3ea586f4c18283a58e117b7bb43bdd4897f373220d08ee83dc1",
      "signature": "(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None"
    },
    "OutputArtifactContract": {
      "kind": "class",
      "members": {
        "unique_roles": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        },
        "validate_cardinality": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "4f754cfeb5d4d5d0a51d99f9a9d9d066ad7a0517a1a2dd82862bd72810813f4e",
      "signature": "(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], minimum: Annotated[int, Ge(ge=0)] = 1, maximum: Annotated[int | None, Ge(ge=1)] = None, derived_from_roles: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)]) -> None"
    },
    "OutputArtifactRoleCount": {
      "kind": "class",
      "schema_sha256": "932e988623cd6631a9f8d0b64a0098cab1ac4a1f70d8609fa8b89676f3c82caf",
      "signature": "(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], count: Annotated[int, Ge(ge=1)]) -> None"
    },
    "OutputArtifactSetIdentity": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, artifacts: 'tuple[OutputArtifact, ...]') -> 'OutputArtifactSetIdentity'"
        },
        "seal_iterable": {
          "kind": "classmethod",
          "signature": "(cls, artifacts: 'Iterable[OutputArtifact]') -> 'OutputArtifactSetIdentity'"
        },
        "validate_summary": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "75c833d938ce913e50ba083b5326c6ddf408d2de5335bbb1872610b2c298dcbe",
      "signature": "(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], roles: Annotated[tuple[stove0_target_protocol.protocol.OutputArtifactRoleCount, ...], MinLen(min_length=1)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "OutputCollectionRef": {
      "kind": "class",
      "schema_sha256": "5e114f49bd35594b81763b7a7bd469021694265597f256e3648d44f6284201e1",
      "signature": "(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], derivation_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "OutputSourceEdge": {
      "kind": "class",
      "schema_sha256": "972b082324ce99560dcf290aaa2190dca0d8d4ad38ed3ae820655588e94db2f4",
      "signature": "(*, output_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')]) -> None"
    },
    "SHA256_PATTERN": {
      "kind": "constant",
      "value": "^[0-9a-f]{64}$"
    },
    "SemanticId": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "SemanticIntentConformanceVector": {
      "kind": "class",
      "schema_sha256": "1675a210db6ad38f3c9e21f6aea633cfa9d851dcbaa8a1d10cac300978ad9670",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, intent: dict[str, JsonValue]) -> None"
    },
    "SemanticIntentConformanceVectors": {
      "kind": "class",
      "members": {
        "canonical_vectors": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[SemanticIntentConformanceVector, ...]') -> 'tuple[SemanticIntentConformanceVector, ...]'"
        },
        "covers_acceptance_and_rejection": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "sha256": {
          "kind": "property",
          "signature": "(self) -> 'str'"
        }
      },
      "schema_sha256": "97bacfee1ce36f2e715ebe95a8f92c7553faaeda88e9080ae8b07a33a04165b0",
      "signature": "(*, format: Literal['stove0-semantic-intent-conformance/v1'] = 'stove0-semantic-intent-conformance/v1', profile_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], vectors: Annotated[tuple[stove0_target_protocol.conformance.SemanticIntentConformanceVector, ...], MinLen(min_length=2)]) -> None"
    },
    "SemanticValidationProfile": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'SemanticValidationProfilePayload') -> 'SemanticValidationProfile'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "6b8688e96bcf912f0eab842d27ff10cea7bc3bb1b53f4b5510968ba90d9d1c9b",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, profile_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "SemanticValidationProfilePayload": {
      "kind": "class",
      "members": {
        "canonical_rules": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        }
      },
      "schema_sha256": "9feba86c6b0478a980740aa7d15a0534fd499479fc164ec3f3b3f3b72b5182f9",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], rules: Annotated[tuple[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], ...], MinLen(min_length=1)], conformance_vectors_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None) -> None"
    },
    "Sha256": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "TARGET_CALLBACK_HTTP_OPERATIONS": {
      "kind": "object",
      "type": "builtins.tuple"
    },
    "TARGET_HTTP_OPERATIONS": {
      "kind": "object",
      "type": "builtins.tuple"
    },
    "TARGET_INPUT_PAGE_MAX": {
      "kind": "constant",
      "value": 256
    },
    "TRANSFORM_TARGET_PROTOCOL": {
      "kind": "constant",
      "value": "stove0-transform-target/v1"
    },
    "TargetCallbackAccess": {
      "kind": "class",
      "schema_sha256": "b19c85f349461f7bea0f4a19f4912f9f84ad2465cca72d713ad8feebea0854ac",
      "signature": "(*, stove0_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None"
    },
    "TargetCallbackAcknowledgement": {
      "kind": "class",
      "schema_sha256": "888aeb6259ebbfb946fb389f43530f2e7f42b0aaa47127fc93cde9b46c06ccfc",
      "signature": "(*, accepted: Literal[True] = True) -> None"
    },
    "TargetContract": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'TargetContractPayload') -> 'TargetContract'"
        },
        "support_for": {
          "kind": "method",
          "signature": "(self, operation_id: 'str') -> 'TargetOperationSupport'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "444d776c4e61c75ff1edfdfeac88aa1e2ca815679fc44af9eae1992e4dde5bf0",
      "signature": "(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "TargetContractPayload": {
      "kind": "class",
      "members": {
        "bind_result_kind": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "canonical_operations": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'"
        }
      },
      "schema_sha256": "ee590c3be91952c47c3452289eef103fef83febe06ce4775481be63c2b40d821",
      "signature": "(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', operations: Annotated[tuple[stove0_target_protocol.protocol.TargetOperationSupport, ...], MinLen(min_length=1)]) -> None"
    },
    "TargetDeclaration": {
      "kind": "class",
      "schema_sha256": "2799fc17c1e3def4ba574b3d4ac58984d664b817f1a646a63e4f35c7343b6997",
      "signature": "(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>) -> None"
    },
    "TargetExecutionEvidence": {
      "kind": "class",
      "schema_sha256": "f76617128b8960e01b2b657a4031d97d1632c188c092473b906be914186ef931",
      "signature": "(*, target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], execution_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], runtime: dict[str, JsonValue] = <factory>) -> None"
    },
    "TargetFailure": {
      "kind": "class",
      "schema_sha256": "1f3471f4274fa0dc57b46bac02964b1e6b6f07bbd8a9a19f618b2031ccb0c065",
      "signature": "(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None"
    },
    "TargetInapplicable": {
      "kind": "class",
      "schema_sha256": "e6e7b7f0a6d88757ed930e374dc9c6877fd96d32d1a689c2d8fbc8e6a3838457",
      "signature": "(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None"
    },
    "TargetInputAuthority": {
      "kind": "class",
      "members": {
        "from_selection": {
          "kind": "classmethod",
          "signature": "(cls, selection: 'ArtifactSelection') -> 'TargetInputAuthority'"
        },
        "validate_summary": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "298824eba461e939612c67d2aac898d33a4faf78cf0b5f562054c5e227bc92ba",
      "signature": "(*, selection: stove0_protocol.fork_join.ArtifactSelectionRef, roles: Annotated[tuple[stove0_target_protocol.protocol.TargetInputRoleCount, ...], MinLen(min_length=1)]) -> None"
    },
    "TargetInputPage": {
      "kind": "class",
      "members": {
        "bind_page": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "7131a1bfdc14187626d6da1e2ed0c2ab5386ccb022b71b5a173812612214dcb0",
      "signature": "(*, authority: stove0_target_protocol.protocol.TargetInputAuthority, continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, next_continuation: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, complete: bool, artifacts: Annotated[tuple[stove0_target_protocol.protocol.InputArtifact, ...], MaxLen(max_length=256)]) -> None"
    },
    "TargetInputRoleCount": {
      "kind": "class",
      "schema_sha256": "cd93b356fb498257b255f7262952a01f1fa657555e9644325bdee9e3c4f14028",
      "signature": "(*, role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], count: Annotated[int, Ge(ge=1)]) -> None"
    },
    "TargetJobDeclaration": {
      "kind": "class",
      "members": {
        "bind_execution": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        },
        "canonical_claim_id": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "a0db588dffc46f0fcadc83e704edb92879ec0aa3f006d9c2d369f36c76e0f4a9",
      "signature": "(*, job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], controller_evidence: stove0_protocol.models.ControllerEvidence, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None"
    },
    "TargetJobRequest": {
      "kind": "class",
      "members": {
        "accepted": {
          "kind": "method",
          "signature": "(self) -> 'AcceptedTargetJob'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, declaration: 'TargetJobDeclaration', runtime: 'TargetRuntimeAuthority', callback_access: 'TargetCallbackAccess') -> 'TargetJobRequest'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "a9548cb0b668b182a9fdebb904bc5e91d9299131bbf9c38cff8a5c19c450063e",
      "signature": "(*, declaration: stove0_target_protocol.protocol.TargetJobDeclaration, runtime: stove0_target_protocol.protocol.TargetRuntimeAuthority, callback_access: stove0_target_protocol.protocol.TargetCallbackAccess, request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "TargetJobState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "TargetJobStatus": {
      "kind": "class",
      "members": {
        "canonical_derivation": {
          "kind": "classmethod",
          "signature": "(cls, value: 'dict[str, Any] | None') -> 'dict[str, Any] | None'"
        },
        "validate_terminal_shape": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "c29a6125c1975f597c8de13fed65f3aa4e4286f4616fa74161074c7217e67684",
      "signature": "(*, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['queued', 'running', 'canceling', 'interrupted', 'inapplicable', 'succeeded', 'failed', 'canceled'], attempt: Annotated[int, Ge(ge=1)], request_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], progress: stove0_target_protocol.protocol.TargetProgress, production: stove0_target_protocol.protocol.TargetProductionAuthority | None = None, output_collection: stove0_target_protocol.protocol.OutputCollectionRef | None = None, execution_evidence: stove0_target_protocol.protocol.TargetExecutionEvidence | None = None, derivation: dict[str, typing.Any] | None = None, effect_receipt: stove0_target_protocol.protocol.ExternalEffectReceipt | None = None, failure: stove0_target_protocol.protocol.TargetFailure | None = None, inapplicable: stove0_target_protocol.protocol.TargetInapplicable | None = None) -> None"
    },
    "TargetOperationSupport": {
      "kind": "class",
      "schema_sha256": "6d124a4d31728580104c9df9fec11f2f903e65acc80d328b6449bf5ac33d4def",
      "signature": "(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], result_kind: Literal['collection', 'external-effect'] = 'collection', options_schema: stove0_protocol.models.JsonSchemaDocument) -> None"
    },
    "TargetOutputBinding": {
      "kind": "class",
      "schema_sha256": "099e02fceaedad912ee5ed40bf980f2c671acea4d76a233dc4128e1fc11f5fdb",
      "signature": "(*, output_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_target_protocol.protocol.OutputCollectionRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None"
    },
    "TargetOutputBindingSetIdentity": {
      "kind": "class",
      "schema_sha256": "daf38f19dc312122935afd84d58bf963fa26e233d8633d1f209196e4f92d8e9a",
      "signature": "(*, artifact_count: Annotated[int, Ge(ge=1)], total_bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "TargetPlan": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "TargetPreflightRequest": {
      "kind": "class",
      "members": {
        "canonical_observations": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ObservationEvidence, ...]') -> 'tuple[ObservationEvidence, ...]'"
        }
      },
      "schema_sha256": "c8edfde8d715a2d94c2d913d1e5e0d71a5b583830587bb7712098fa3b1ce301f",
      "signature": "(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1', 'stove0-effect-target/v1'] = 'stove0-transform-target/v1', observations: tuple[stove0_protocol.models.ObservationEvidence, ...] = ()) -> None"
    },
    "TargetPreflightResponse": {
      "kind": "class",
      "members": {
        "bind_protocol": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "2de154a75245f705341e8f659ce900ce6f3b2bef59a16b22d7980249391824e9",
      "signature": "(*, target: stove0_target_protocol.protocol.TargetContract, plan: stove0_target_protocol.protocol.TransformPlan | stove0_target_protocol.protocol.EffectPlan) -> None"
    },
    "TargetProductionAuthority": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'TargetProductionAuthorityPayload') -> 'TargetProductionAuthority'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "955edae0149fdeb8c282f51dd1687bbaab9a525e46b05d22ef13f7fd5a9fc423",
      "signature": "(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity, production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "TargetProductionAuthorityPayload": {
      "kind": "class",
      "schema_sha256": "33f746fc9f49278dbbf1462ee186ee0e11fe95c8830e1a6d406631252d405022",
      "signature": "(*, format: Literal['stove0-target-production/v1'] = 'stove0-target-production/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], outputs: stove0_target_protocol.protocol.OutputArtifactSetIdentity, disposition_count: Annotated[int, Ge(ge=1)], disposition_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], source_edge_count: Annotated[int, Ge(ge=1)], source_edge_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], riverhog_disposition_set: riverhog_protocol.collection_workflows.ArtifactDispositionSetIdentity) -> None"
    },
    "TargetProductionSealResponse": {
      "kind": "class",
      "members": {
        "validate_state": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "6f7a13f1208812f0f424f11b7006ea462410222ff9b0e962c602c71eabf08e3c",
      "signature": "(*, state: Literal['sealing', 'sealed'], production: stove0_target_protocol.protocol.TargetProductionAuthority | None = None) -> None"
    },
    "TargetProgress": {
      "kind": "class",
      "members": {
        "validate_total": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "3b531b19161c4e085385a0c27312bc32072b1dda15095512891696fd64be3fca",
      "signature": "(*, phase: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], completed: Annotated[int, Ge(ge=0)], total: Annotated[int | None, Ge(ge=0)] = None, unit: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=40)] = None) -> None"
    },
    "TargetProtocol": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "TargetProtocolModel": {
      "kind": "class",
      "schema_sha256": "c93552e6289487dd5fa8baeb3416b24ed8657c354167fb0ed7a8de1e1a8ccb90",
      "signature": "() -> None"
    },
    "TargetResultKind": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "TargetRuntimeAuthority": {
      "kind": "class",
      "schema_sha256": "6ac4b25b7019574c8a208af672bd8cd7caa8a1b22ac48d6ddf6946379a4bafa7",
      "signature": "(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False) -> None"
    },
    "TargetSettlementAuthority": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'TargetSettlementAuthorityPayload') -> 'TargetSettlementAuthority'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "27d5c82cd51c2f39dfd46a39cd6be0c7ba647be6162e468f45697fca4d1964de",
      "signature": "(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity, settlement_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "TargetSettlementAuthorityPayload": {
      "kind": "class",
      "schema_sha256": "d071532afb993c200c67017bc86e50fb52b9d8d358989eac205da3e22f0c6142",
      "signature": "(*, format: Literal['stove0-target-settlement/v1'] = 'stove0-target-settlement/v1', job_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], production_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], output_collection: stove0_target_protocol.protocol.OutputCollectionRef, output_bindings: stove0_target_protocol.protocol.TargetOutputBindingSetIdentity) -> None"
    },
    "TransformPlan": {
      "kind": "class",
      "members": {
        "binding_document": {
          "kind": "method",
          "signature": "(self) -> 'dict[str, JsonValue]'"
        },
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'TransformPlanPayload') -> 'TransformPlan'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "6f59b836f3424c3b6ad464e31db0949cba1c594122b94d6c7adf6d756ca618c7",
      "signature": "(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1'] = 'stove0-transform-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = (), plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "TransformPlanPayload": {
      "kind": "class",
      "members": {
        "canonical_observation_results": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'"
        }
      },
      "schema_sha256": "aeb7f795159621c7f469ce34008b3a33414d8f519c71753d3b355f2e2489efcc",
      "signature": "(*, operation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], operation_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], inputs: stove0_target_protocol.protocol.TargetInputAuthority, intent: dict[str, JsonValue], target_options: dict[str, JsonValue] = <factory>, protocol: Literal['stove0-transform-target/v1'] = 'stove0-transform-target/v1', target_implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], target_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observation_result_sha256s: tuple[typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], ...] = ()) -> None"
    },
    "WorkspaceAssurance": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "canonical_json_bytes": {
      "kind": "function",
      "signature": "(value: 'object') -> 'bytes'"
    },
    "canonical_json_sha256": {
      "kind": "function",
      "signature": "(value: 'object') -> 'str'"
    },
    "update_input_disposition_commitment": {
      "kind": "function",
      "signature": "(digest: 'Any', *, ordinal: 'int', disposition: 'InputDispositionDeclaration') -> 'None'"
    },
    "update_output_artifact_commitment": {
      "kind": "function",
      "signature": "(digest: 'Any', *, ordinal: 'int', artifact: 'OutputArtifact') -> 'None'"
    },
    "update_output_source_edge_commitment": {
      "kind": "function",
      "signature": "(digest: 'Any', *, ordinal: 'int', edge: 'OutputSourceEdge') -> 'None'"
    },
    "update_target_output_binding_commitment": {
      "kind": "function",
      "signature": "(digest: 'Any', *, ordinal: 'int', binding: 'TargetOutputBinding') -> 'None'"
    },
    "validate_declaration_against_operation": {
      "kind": "function",
      "signature": "(declaration: 'TargetDeclaration', operation: 'OperationContract') -> 'None'"
    },
    "validate_preflight_response_against_request": {
      "kind": "function",
      "signature": "(response: 'TargetPreflightResponse', request: 'TargetPreflightRequest') -> 'None'"
    },
    "validate_status_against_request": {
      "kind": "function",
      "signature": "(status: 'TargetJobStatus', request: 'TargetJobRequest | AcceptedTargetJob', operation: 'OperationContract') -> 'None'"
    }
  },
  "module": "stove0_target_protocol"
}
```
