# stove0_review_target_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support:9b67bd7ad3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-fad772c4cc) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d542b6be53"></a>
| Field | Shape |
|---|---|
| <a id="s-ad016668fc"></a>`candidate_id` | "python:stove0-review-target-support:stove0_review_target_support" |
| <a id="s-6cd4dc45f9"></a>`distribution` | "stove0-review-target-support" |
| <a id="s-e403d92aaa"></a>`exports` | additional keys=`ReviewTargetConfig`, `ReviewTargetServiceBase`, `SamplerConfig`, `SamplerRegistration`, `create_target_app`, `file_identity`, `load_sampler_registrations`, `parse_sampler_registrations`, `review_options_schema` |
| <a id="s-1be3f046a9"></a>`module` | "stove0_review_target_support" |

## Governing policies

- <a id="pa-10ec596909"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/56`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9bcfdd2b500a6fb1e4b398248bed7feb03b85f14c865736d9b99c2885c357ea6 -->

```json
{
  "candidate_id": "python:stove0-review-target-support:stove0_review_target_support",
  "distribution": "stove0-review-target-support",
  "exports": {
    "ReviewTargetConfig": {
      "kind": "class",
      "members": {
        "canonical_samplers": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'tuple[SamplerConfig, ...]') -> 'tuple[SamplerConfig, ...]'\""
        }
      },
      "schema_sha256": "465ba4684267b5a403b655bcbaaa58dbe51e259b14ffa6dc2fbd902af816ff0c",
      "signature": "'(*, samplers: Annotated[tuple[stove0_review_target_support.app.SamplerConfig, ...], MinLen(min_length=1)]) -> None'"
    },
    "ReviewTargetServiceBase": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "\"(self) -> 'None'\""
        },
        "preflight": {
          "kind": "method",
          "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
        },
        "readiness": {
          "kind": "method",
          "signature": "\"(self) -> 'dict[str, str]'\""
        }
      },
      "signature": "\"(*, state_root: 'Path', workspace_root: 'Path', samplers: 'tuple[SamplerRegistration, ...]', source_revision: 'str' = 'unknown', image_digest: 'str', implementation_version: 'str', protocol: 'TargetProtocol', implementation_id: 'str', operation: 'OperationContract', options_schema: 'JsonSchemaDocument', terminal_state_retention_seconds: 'int' = 2592000) -> 'None'\""
    },
    "SamplerConfig": {
      "kind": "class",
      "members": {
        "absolute_token_file": {
          "kind": "classmethod",
          "signature": "\"(cls, value: 'Path') -> 'Path'\""
        }
      },
      "schema_sha256": "f41a6e541ec173a2e5d15cf28386e186b3440a8e7345c69f22509c6add3dad8e",
      "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$')], base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token_file: pathlib.Path, allow_insecure_http: bool = False, descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
    },
    "SamplerRegistration": {
      "fields": [
        {
          "default": "required",
          "name": "id",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "client",
          "type": "'ReviewSamplerClient'"
        },
        {
          "default": "required",
          "name": "descriptor_sha256",
          "type": "'str'"
        },
        {
          "default": "required",
          "name": "image_digest",
          "type": "'str'"
        }
      ],
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self) -> 'SamplerDescriptor'\""
        }
      },
      "signature": "\"(id: 'str', client: 'ReviewSamplerClient', descriptor_sha256: 'str', image_digest: 'str') -> None\""
    },
    "create_target_app": {
      "kind": "function",
      "signature": "\"(*, service: 'str', title: 'str', token: 'str', target: 'ReviewTarget') -> 'FastAPI'\""
    },
    "file_identity": {
      "kind": "function",
      "signature": "\"(path: 'Path') -> 'tuple[int, str]'\""
    },
    "load_sampler_registrations": {
      "kind": "function",
      "signature": "\"(path: 'Path') -> 'tuple[SamplerRegistration, ...]'\""
    },
    "parse_sampler_registrations": {
      "kind": "function",
      "signature": "\"(document: 'str') -> 'tuple[SamplerRegistration, ...]'\""
    },
    "review_options_schema": {
      "kind": "function",
      "signature": "\"(schema_id: 'str', *, required: 'tuple[str, ...]' = (), properties: 'Mapping[str, JsonValue] | None' = None) -> 'JsonSchemaDocument'\""
    }
  },
  "module": "stove0_review_target_support"
}
```
