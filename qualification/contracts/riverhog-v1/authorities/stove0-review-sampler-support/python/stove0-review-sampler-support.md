# stove0_review_sampler_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-support:stove0-review-sampler-support:0de7f9368d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-571cb480ec"></a>
| Field | Shape |
|---|---|
| <a id="s-2ebf186f02"></a>`candidate_id` | "python:stove0-review-sampler-support:stove0_review_sampler_support" |
| <a id="s-3c7c8d5d15"></a>`distribution` | "stove0-review-sampler-support" |
| <a id="s-c567855cdc"></a>`exports` | additional keys=`ReviewSampler`, `SAMPLER_CONFORMANCE_RESULT`, `SAMPLER_HTTP_OPERATIONS`, `SAMPLER_SCHEMA_BUNDLE_FORMAT`, `SamplerClient`, `SamplerConformanceResult`, `SamplerHttpBinding`, `SamplerHttpResponse`, `SamplerWorkspace`, `conformance_report`, `sampler_schema_bundle` |
| <a id="s-5945b9bd09"></a>`module` | "stove0_review_sampler_support" |

## Governing policies

- <a id="pa-4359561984"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-sampler-support:stove0_review_sampler_support](../../../evidence/sources.md#src-6dd798b0df) — `reference/stove0/targets/review/sampler/support/src/stove0_review_sampler_support/__init__.py`

### Machine authority

- `/external_contract/python/54`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b694eefdff5b609765fc6149fb654d287e956d64e71a49889a7b5440fca3fe9b -->

```json
{
  "candidate_id": "python:stove0-review-sampler-support:stove0_review_sampler_support",
  "distribution": "stove0-review-sampler-support",
  "exports": {
    "ReviewSampler": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self) -> 'SamplerDescriptor'\""
        },
        "sample": {
          "kind": "method",
          "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "SAMPLER_CONFORMANCE_RESULT": {
      "kind": "constant",
      "value": "stove0-review-sampler-conformance-result/v1"
    },
    "SAMPLER_HTTP_OPERATIONS": {
      "kind": "object",
      "type": "builtins.tuple"
    },
    "SAMPLER_SCHEMA_BUNDLE_FORMAT": {
      "kind": "constant",
      "value": "stove0-review-sampler-schema-bundle/v1"
    },
    "SamplerClient": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""
        },
        "sample": {
          "kind": "method",
          "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
        }
      },
      "signature": "'(*args, **kwargs)'"
    },
    "SamplerConformanceResult": {
      "kind": "class",
      "members": {
        "validate_result": {
          "kind": "method",
          "signature": "\"(self) -> 'Self'\""
        }
      },
      "schema_sha256": "04925f6f55b8bdb122d1cfdcba5aa12da892ebae3bdc0f0f0f6955364773c80f",
      "signature": "\"(*, format: Literal['stove0-review-sampler-conformance-result/v1'] = 'stove0-review-sampler-conformance-result/v1', status: Literal['conformant', 'inspected'], sampler: stove0_review_sampler_protocol.SamplerDescriptor, coverage: stove0_review_sampler_support.conformance.SamplerConformanceCoverage, sampling: Literal['exercised', 'not-exercised'], request: stove0_review_sampler_protocol.SamplerRequest | None = None, sample: stove0_review_sampler_protocol.SamplerResult | None = None) -> None\""
    },
    "SamplerHttpBinding": {
      "kind": "class",
      "members": {
        "handle": {
          "kind": "method",
          "signature": "\"(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'SamplerHttpResponse'\""
        }
      },
      "signature": "\"(sampler: 'ReviewSampler', *, maximum_request_bytes: 'int' = 4194304, maximum_concurrency: 'int' = 1) -> 'None'\""
    },
    "SamplerHttpResponse": {
      "fields": [
        {
          "default": "required",
          "name": "status",
          "type": "'int'"
        },
        {
          "default": "required",
          "name": "headers",
          "type": "'tuple[tuple[str, str], ...]'"
        },
        {
          "default": "required",
          "name": "body",
          "type": "'bytes'"
        }
      ],
      "kind": "class",
      "signature": "\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""
    },
    "SamplerWorkspace": {
      "kind": "class",
      "members": {
        "canceled": {
          "kind": "method",
          "signature": "\"(self) -> 'bool'\""
        },
        "output": {
          "kind": "method",
          "signature": "\"(self, relative_path: 'str') -> 'Path'\""
        },
        "resolve": {
          "kind": "method",
          "signature": "\"(self, relative_path: 'str') -> 'Path'\""
        },
        "verify_input": {
          "kind": "method",
          "signature": "\"(self, declared: 'SamplerInput') -> 'Path'\""
        }
      },
      "signature": "\"(root: 'Path', request: 'SamplerRequest') -> 'None'\""
    },
    "conformance_report": {
      "kind": "function",
      "signature": "\"(client: 'SamplerClient', *, request: 'SamplerRequest | None' = None) -> 'SamplerConformanceResult'\""
    },
    "sampler_schema_bundle": {
      "kind": "function",
      "signature": "\"() -> 'dict[str, Any]'\""
    }
  },
  "module": "stove0_review_sampler_support"
}
```
