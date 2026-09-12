# stove0_observer_protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol:5ae8e13a79 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-protocol` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/17`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:stove0-observer-protocol` — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `distribution` | "stove0-observer-protocol" |
| `exports` | object (43 fields) |
| `module` | "stove0_observer_protocol" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 25c1fea3ee7675e36d988feea00b9c5b1f347212f5d65bee0142f5ad6834e889 -->

```json
{
  "distribution": "stove0-observer-protocol",
  "exports": {
    "ARTIFACT_ID_PATTERN": {
      "kind": "constant",
      "value": "^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$"
    },
    "ArtifactSubject": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "e232a8cd82feef9c0182803e046cc2d761c6cc9a66de08fcb1aa58eb275a86c6",
      "signature": "(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], role: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], collection: stove0_protocol.models.CollectionRootRef, path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None"
    },
    "CollectionRootRef": {
      "kind": "class",
      "members": {
        "from_identity": {
          "kind": "classmethod",
          "signature": "(cls, value: 'CollectionRootIdentity') -> 'CollectionRootRef'"
        },
        "to_identity": {
          "kind": "method",
          "signature": "(self) -> 'CollectionRootIdentity'"
        }
      },
      "schema_sha256": "0129b21c7d9e83eb25d287865778230c2388dd72c6a5fa8243a72b6c74a72649",
      "signature": "(*, collection_id: CollectionId, archive_root_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], content_identity: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "FactsSemanticValidator": {
      "kind": "object",
      "type": "collections.abc._CallableGenericAlias"
    },
    "JSON_SCHEMA_ONLY_SEMANTIC_PROFILE": {
      "kind": "object",
      "type": "stove0_protocol.models.SemanticValidationProfile"
    },
    "JsonSchemaDocument": {
      "kind": "class",
      "members": {
        "from_schema": {
          "kind": "classmethod",
          "signature": "(cls, schema_id: 'str', schema: 'dict[str, JsonValue]') -> 'JsonSchemaDocument'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "044aac56d1ab78dca2a949caf25694292222a652e8b22484284cfd16c793e05f",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None"
    },
    "OBSERVATION_REQUEST_FORMAT": {
      "kind": "constant",
      "value": "stove0-observation-request/v1"
    },
    "OBSERVATION_RESULT_FORMAT": {
      "kind": "constant",
      "value": "stove0-observation-result/v1"
    },
    "OBSERVER_HTTP_OPERATIONS": {
      "kind": "object",
      "type": "builtins.tuple"
    },
    "OBSERVER_PROTOCOL": {
      "kind": "constant",
      "value": "stove0-content-observer/v1"
    },
    "ObservationEvidence": {
      "kind": "class",
      "members": {
        "bind_result": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "fe203426a6a155925058ab72a4cdf1c5ed682d85bae5b822f1a1f67bfee09fc9",
      "signature": "(*, request: stove0_protocol.models.ObservationRequest, result: stove0_protocol.models.ObservationResult) -> None"
    },
    "ObservationFailure": {
      "kind": "class",
      "schema_sha256": "03452b85273ec047f313b026f245c633c5677b66c3c74316258933f63c051c7f",
      "signature": "(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None"
    },
    "ObservationInapplicable": {
      "kind": "class",
      "schema_sha256": "95bfa40604f4246ad72c306da05ea43a9d657940bcf1ec5706fe8073e81748bd",
      "signature": "(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None"
    },
    "ObservationInvocation": {
      "kind": "class",
      "members": {
        "canonical_claim_id": {
          "kind": "classmethod",
          "signature": "(cls, value: 'str') -> 'str'"
        }
      },
      "schema_sha256": "82cb342e57cc6456dc2d8acdb09d8836f7be2d23c0d8d5ab5ce8d23a5d8c32d7",
      "signature": "(*, request: stove0_protocol.models.ObservationRequest, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)], runtime: stove0_protocol.models.ObserverRuntimeAuthority) -> None"
    },
    "ObservationRequest": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'ObservationRequestPayload') -> 'ObservationRequest'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "35ac3c22e749ed4adca72bf8e0e2afea59cd948e7b36f0950b0124f574e5ab28",
      "signature": "(*, format: Literal['stove0-observation-request/v1'] = 'stove0-observation-request/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], observer_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ObservationRequestPayload": {
      "kind": "class",
      "members": {
        "canonical_subjects": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'"
        }
      },
      "schema_sha256": "9abfa9a4aecfcbe425bfb30526b9f1155f938183125313040afd41257ac9e095",
      "signature": "(*, format: Literal['stove0-observation-request/v1'] = 'stove0-observation-request/v1', work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_registration_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9.-]{0,118}[a-z0-9])?$', ascii_only=None)], observer_descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)] = 300, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, retrieval_policy: Literal['available-only', 'allow'] = 'available-only') -> None"
    },
    "ObservationResult": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'ObservationResultPayload') -> 'ObservationResult'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "187dbdacb86a1a9ac08397f119e776ab40b1baa47a06c393b3bd21b842ac2753",
      "signature": "(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaDocument | None = None, facts: dict[str, JsonValue] | None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ObservationInapplicable | None = None, failure: stove0_protocol.models.ObservationFailure | None = None, result_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ObservationResultPayload": {
      "kind": "class",
      "members": {
        "canonical_subjects": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'"
        },
        "validate_state_payload": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "1a1aa64006d73d7ae46bba02f9833cc66a623d6c3fe11d9209145c1e374810ed",
      "signature": "(*, format: Literal['stove0-observation-result/v1'] = 'stove0-observation-result/v1', request_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], state: Literal['observed', 'inapplicable', 'failed', 'canceled'], observer: stove0_protocol.models.ObserverImplementation, observer_contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], observer_contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], facts_schema: stove0_protocol.models.JsonSchemaDocument | None = None, facts: dict[str, JsonValue] | None = None, facts_sha256: Optional[Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]] = None, execution_evidence: dict[str, JsonValue] = <factory>, inapplicable: stove0_protocol.models.ObservationInapplicable | None = None, failure: stove0_protocol.models.ObservationFailure | None = None) -> None"
    },
    "ObservationState": {
      "kind": "object",
      "type": "typing._LiteralGenericAlias"
    },
    "ObserverContract": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'ObserverContractPayload') -> 'ObserverContract'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "79326d184bed03d400708fc7fca5dca34ea84a3c02cb3e536cd3788f1daf1ff7",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576, contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ObserverContractPayload": {
      "kind": "class",
      "members": {
        "bind_semantic_conformance_vectors": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "ed174398a0f2af336d7aa0f8dee516b6c7bbc8738fc13f26411e98bcb9f66df0",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)] = 1048576) -> None"
    },
    "ObserverContractSupport": {
      "kind": "class",
      "members": {
        "from_contract": {
          "kind": "classmethod",
          "signature": "(cls, value: 'ObserverContract', *, preferred_subject_batch_size: 'int' = 128) -> 'ObserverContractSupport'"
        }
      },
      "schema_sha256": "fab7dc8d2c5290e8e1fc0607fffbbdea2505809a70a027f3e882712313fada0b",
      "signature": "(*, contract_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], contract_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], options_schema: stove0_protocol.models.JsonSchemaDocument, facts_schema: stove0_protocol.models.JsonSchemaDocument, facts_semantics: stove0_protocol.models.SemanticValidationProfile, preferred_subject_batch_size: Annotated[int, Ge(ge=1)] = 128, maximum_result_bytes: Annotated[int, Ge(ge=1), Le(le=67108864)]) -> None"
    },
    "ObserverDescriptor": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "(cls, payload: 'ObserverDescriptorPayload') -> 'ObserverDescriptor'"
        },
        "support_for": {
          "kind": "method",
          "signature": "(self, contract_id: 'str') -> 'ObserverContractSupport'"
        },
        "verify_digest": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "5717766018649fca58b8d29b78adfc61b4a572a9a7d959ac997b6f9756460f41",
      "signature": "(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ObserverDescriptorPayload": {
      "kind": "class",
      "members": {
        "unique_contracts": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ObserverContractSupport, ...]') -> 'tuple[ObserverContractSupport, ...]'"
        }
      },
      "schema_sha256": "96b0440bb530423fbeb0b6687c18f7f2fff67acc33d26df0e36018d29674e8ae",
      "signature": "(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', implementation_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], contracts: Annotated[tuple[stove0_protocol.models.ObserverContractSupport, ...], MinLen(min_length=1)]) -> None"
    },
    "ObserverImplementation": {
      "kind": "class",
      "schema_sha256": "6ffdaa644c4d7b3d7f3e17d66ebec04a5997be6e6e59c8839ae0685b1250ba75",
      "signature": "(*, protocol: Literal['stove0-content-observer/v1'] = 'stove0-content-observer/v1', id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], descriptor_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None"
    },
    "ObserverRuntimeAuthority": {
      "kind": "class",
      "schema_sha256": "a99782500110e8937b85f3ce1843790ca1e5b7febec3c08000c176c5452cc51c",
      "signature": "(*, transport: Literal['riverhog-capability/v1'] = 'riverhog-capability/v1', riverhog_base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], capability_token: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], allow_insecure_http: bool = False, workspace_assurance: Literal['encrypted', 'ephemeral']) -> None"
    },
    "RIVERHOG_CAPABILITY_TRANSPORT": {
      "kind": "constant",
      "value": "riverhog-capability/v1"
    },
    "SHA256_PATTERN": {
      "kind": "constant",
      "value": "^[0-9a-f]{64}$"
    },
    "SemanticFactsConformanceVector": {
      "kind": "class",
      "members": {
        "canonical_subjects": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'"
        }
      },
      "schema_sha256": "7d466116d6fc7df4a6299063bc8a4f2729dc8471c4f7a0144f7da4a250c34ba3",
      "signature": "(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], accepted: bool, subjects: Annotated[tuple[stove0_protocol.models.ArtifactSubject, ...], MinLen(min_length=1)], options: dict[str, JsonValue] = <factory>, facts: dict[str, JsonValue]) -> None"
    },
    "SemanticFactsConformanceVectors": {
      "kind": "class",
      "members": {
        "canonical_vectors": {
          "kind": "classmethod",
          "signature": "(cls, value: 'tuple[SemanticFactsConformanceVector, ...]') -> 'tuple[SemanticFactsConformanceVector, ...]'"
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
      "schema_sha256": "fa3edfab36bf499bb8091bc692ce8b2b46628ada7c9db518458ffd8f02fb1aa7",
      "signature": "(*, format: Literal['stove0-semantic-facts-conformance/v1'] = 'stove0-semantic-facts-conformance/v1', profile_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], vectors: Annotated[tuple[stove0_observer_protocol.conformance.SemanticFactsConformanceVector, ...], MinLen(min_length=2)]) -> None"
    },
    "SemanticId": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
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
    "SemanticValidatorBinding": {
      "fields": [
        {
          "default": "required",
          "name": "profile_id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "profile_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "validator",
          "type": "'FactsSemanticValidator'"
        }
      ],
      "kind": "class",
      "members": {
        "from_profile": {
          "kind": "classmethod",
          "signature": "(cls, profile: 'SemanticValidationProfile', validator: 'FactsSemanticValidator') -> 'SemanticValidatorBinding'"
        }
      },
      "signature": "(profile_id: 'str', profile_sha256: 'str', validator: 'FactsSemanticValidator') -> None"
    },
    "SemanticValidatorProvider": {
      "kind": "class",
      "members": {
        "resolve": {
          "kind": "method",
          "signature": "(self, profile_id: 'str', profile_sha256: 'str') -> 'FactsSemanticValidator | None'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "SemanticValidatorRegistry": {
      "kind": "class",
      "members": {
        "resolve": {
          "kind": "method",
          "signature": "(self, profile_id: 'str', profile_sha256: 'str') -> 'FactsSemanticValidator | None'"
        }
      },
      "signature": "(bindings: 'Iterable[SemanticValidatorBinding]' = ()) -> 'None'"
    },
    "Sha256": {
      "kind": "object",
      "type": "typing._AnnotatedAlias"
    },
    "accept_observation_result": {
      "kind": "function",
      "signature": "(result: 'ObservationResult', request: 'ObservationRequest', descriptor: 'ObserverDescriptor', semantic_validators: 'SemanticValidatorProvider | None' = None) -> 'None'"
    },
    "canonical_json_bytes": {
      "kind": "function",
      "signature": "(value: 'object') -> 'bytes'"
    },
    "canonical_json_sha256": {
      "kind": "function",
      "signature": "(value: 'object') -> 'str'"
    },
    "require_semantic_validators": {
      "kind": "function",
      "signature": "(provider: 'SemanticValidatorProvider | None', descriptor: 'ObserverDescriptor') -> 'None'"
    },
    "validate_observation_request": {
      "kind": "function",
      "signature": "(request: 'ObservationRequest', descriptor: 'ObserverDescriptor') -> 'ObserverContractSupport'"
    },
    "validate_observation_result_structure": {
      "kind": "function",
      "signature": "(result: 'ObservationResult', request: 'ObservationRequest', descriptor: 'ObserverDescriptor') -> 'ObserverContractSupport'"
    }
  },
  "module": "stove0_observer_protocol"
}
```
