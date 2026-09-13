# stove0_review_sampler_protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol:355bab843c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3776e025f0"></a>
| Field | Shape |
|---|---|
| <a id="s-985cf88746"></a>`candidate_id` | "python:stove0-review-sampler-protocol:stove0_review_sampler_protocol" |
| <a id="s-b4ac90072a"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-fdc34bd78b"></a>`exports` | additional keys=`SAMPLER_HTTP_OPERATIONS`, `SAMPLER_PROTOCOL`, `SamplerDescriptor`, `SamplerDescriptorPayload`, `SamplerFailure`, `SamplerInapplicable`, `SamplerInput`, `SamplerOutput`, `SamplerRequest`, `SamplerRequestPayload`, `SamplerResult`, `SamplerResultPayload`, `SamplerWindow`, `validate_result` |
| <a id="s-6e330b40ce"></a>`module` | "stove0_review_sampler_protocol" |

## Governing policies

- <a id="pa-547e5eea71"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — `reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py`

### Machine authority

- `/external_contract/python/53`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f5d41a2e53d87cda46d2723c6e6de71fc4b574f99d63facccd3c58ead7fa7f5f -->

```json
{
  "candidate_id": "python:stove0-review-sampler-protocol:stove0_review_sampler_protocol",
  "distribution": "stove0-review-sampler-protocol",
  "exports": {
    "SAMPLER_HTTP_OPERATIONS": {
      "kind": "object",
      "type": "builtins.tuple"
    },
    "SAMPLER_PROTOCOL": {
      "kind": "constant",
      "value": "stove0-review-sampler/v1"
    },
    "SamplerDescriptor": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "\"(cls, payload: 'SamplerDescriptorPayload') -> 'SamplerDescriptor'\""
        },
        "verify_digest": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "1f0d4b42d3d84843e4fc3419d4ad90570af2991916b068a4f7658fc9cd25b077",
      "signature": "\"(*, protocol: Literal['stove0-review-sampler/v1'] = 'stove0-review-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaDocument, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
    },
    "SamplerDescriptorPayload": {
      "kind": "class",
      "schema_sha256": "4b6f48e7eabd1ba6e41412155ab6b12b1ae858a1fb2e7431ed93c92c86d78ff1",
      "signature": "\"(*, protocol: Literal['stove0-review-sampler/v1'] = 'stove0-review-sampler/v1', implementation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], implementation_version: Annotated[str, MinLen(min_length=1), MaxLen(max_length=120)], source_revision: Annotated[str, MinLen(min_length=1), MaxLen(max_length=200)], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], primary_operation_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], primary_operation_contract_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], portable_intent_schema: stove0_protocol.models.JsonSchemaDocument, output_role: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')]) -> None\""
    },
    "SamplerFailure": {
      "kind": "class",
      "schema_sha256": "463469db3cd89d3a6b04230ebb807a8b96ffdae48c5784517cce987ca9fe7ce5",
      "signature": "\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None\""
    },
    "SamplerInapplicable": {
      "kind": "class",
      "schema_sha256": "a5e2ccae9cf0dfad46fa94f700c6994dcc8fe643c1ae44cd945353e8d765ef51",
      "signature": "\"(*, code: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$')], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None\""
    },
    "SamplerInput": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'str') -> 'str'\""
        }
      },
      "schema_sha256": "598e2ba7a086e6cdf7ff6d4260b002f766956002e655230031b8328b253ebd57",
      "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str | None, MinLen(min_length=1), MaxLen(max_length=255)] = None) -> None\""
    },
    "SamplerOutput": {
      "kind": "class",
      "members": {
        "canonical_path": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'str') -> 'str'\""
        },
        "canonical_sources": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
        }
      },
      "schema_sha256": "6e4a50dfd5fcaa493a6baa6f34436ab5b4bc32ab808f0f2c7d6f99fda3befdac",
      "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], bytes: Annotated[int, Ge(ge=0)], sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], media_type: Annotated[str, MinLen(min_length=1), MaxLen(max_length=255)], derived_from: Annotated[tuple[str, ...], MinLen(min_length=1)]) -> None\""
    },
    "SamplerRequest": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "\"(cls, payload: 'SamplerRequestPayload') -> 'SamplerRequest'\""
        },
        "verify_digest": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "c4c0c1f6ca2eeb83c6b5bb7efc469587b92ba815ce556e33a976a98b0d093c9f",
      "signature": "\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)], request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
    },
    "SamplerRequestPayload": {
      "kind": "class",
      "members": {
        "canonical_cancellation_path": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'str') -> 'str'\""
        },
        "canonical_inputs": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[SamplerInput, ...]') -> 'tuple[SamplerInput, ...]'\""
        },
        "canonical_windows": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[SamplerWindow, ...]') -> 'tuple[SamplerWindow, ...]'\""
        },
        "references_exact_inputs": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "72ab9541c3c2ca5d9d11df63bda09ee21d19c7d0f7aa7305749271d6162ac01d",
      "signature": "\"(*, format: Literal['stove0-review-sampler-request/v1'] = 'stove0-review-sampler-request/v1', sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], workspace_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], inputs: Annotated[tuple[stove0_review_sampler_protocol.SamplerInput, ...], MinLen(min_length=1)], windows: Annotated[tuple[stove0_review_sampler_protocol.SamplerWindow, ...], MinLen(min_length=1)], portable_intent: dict[str, JsonValue], maximum_output_bytes: Annotated[int, Ge(ge=1), Le(le=1099511627776)], timeout_seconds: Annotated[int, Ge(ge=1), Le(le=86400)], cancellation_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""
    },
    "SamplerResult": {
      "kind": "class",
      "members": {
        "seal": {
          "kind": "classmethod",
          "signature": "\"(cls, payload: 'SamplerResultPayload') -> 'SamplerResult'\""
        },
        "verify_digest": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "21f6b9d3f23551988794e5c55d6db07545900c3d9b5dfc7b2372acc553d8e0d3",
      "signature": "\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure | None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable | None = None, result_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
    },
    "SamplerResultPayload": {
      "kind": "class",
      "members": {
        "canonical_outputs": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[SamplerOutput, ...]') -> 'tuple[SamplerOutput, ...]'\""
        },
        "state_shape": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "43995295819eee9d7834b9a88b848ddce3aa1e525b17ab129b0571c2eaed661b",
      "signature": "\"(*, format: Literal['stove0-review-sampler-result/v1'] = 'stove0-review-sampler-result/v1', request_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], sampler_descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], state: Literal['succeeded', 'inapplicable', 'failed', 'canceled'], outputs: tuple[stove0_review_sampler_protocol.SamplerOutput, ...] = (), execution_evidence: dict[str, JsonValue] = <factory>, failure: stove0_review_sampler_protocol.SamplerFailure | None = None, inapplicable: stove0_review_sampler_protocol.SamplerInapplicable | None = None) -> None\""
    },
    "SamplerWindow": {
      "kind": "class",
      "members": {
        "canonical_output_path": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'str') -> 'str'\""
        }
      },
      "schema_sha256": "70e05026e2bb99cdfa6d74df5cc6bde72c26a3df7e62152f80345fed5a8d4420",
      "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], input_id: Annotated[str, _PydanticGeneralMetadata(pattern='^[A-Za-z0-9]\u0028?:[A-Za-z0-9._-]{0,158}[A-Za-z0-9])?$')], start_ms: Annotated[int, Ge(ge=0)], duration_ms: Annotated[int, Ge(ge=1)], output_path: Annotated[str, MinLen(min_length=1), MaxLen(max_length=4096)]) -> None\""
    },
    "validate_result": {
      "kind": "function",
      "signature": "\"(result: 'SamplerResult', request: 'SamplerRequest', descriptor: 'SamplerDescriptor') -> 'None'\""
    }
  },
  "module": "stove0_review_sampler_protocol"
}
```
