# stove0_observer_support

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support:05ba78f9d7 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-support` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/18`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:stove0-observer-support` — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

```json
{
  "distribution": "stove0-observer-support",
  "exports": {
    "CancellationCheck": {
      "kind": "object",
      "type": "collections.abc._CallableGenericAlias"
    },
    "ContentObserver": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "(self) -> 'ObserverDescriptor'"
        },
        "observe": {
          "kind": "method",
          "signature": "(self, request: 'ObservationRequest', runtime: 'ObservationRuntime') -> 'ObservationResult'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "FactsSemanticValidator": {
      "kind": "object",
      "type": "collections.abc._CallableGenericAlias"
    },
    "Heartbeat": {
      "kind": "object",
      "type": "collections.abc._CallableGenericAlias"
    },
    "OBSERVER_CONFORMANCE_RESULT": {
      "kind": "constant",
      "value": "stove0-observer-conformance-result/v1"
    },
    "OBSERVER_HTTP_OPERATIONS": {
      "kind": "object",
      "type": "builtins.tuple"
    },
    "OBSERVER_SCHEMA_BUNDLE_FORMAT": {
      "kind": "constant",
      "value": "stove0-observer-schema-bundle/v1"
    },
    "ObservationResultBuilder": {
      "kind": "class",
      "members": {
        "canceled": {
          "kind": "method",
          "signature": "(self, *, execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'"
        },
        "failed": {
          "kind": "method",
          "signature": "(self, *, code: 'str', message: 'str', retryable: 'bool', execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'"
        },
        "inapplicable": {
          "kind": "method",
          "signature": "(self, *, code: 'str', message: 'str', execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'"
        },
        "observed": {
          "kind": "method",
          "signature": "(self, facts: 'Mapping[str, JsonValue]', *, execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'"
        }
      },
      "signature": "(descriptor: 'ObserverDescriptor', request: 'ObservationRequest') -> 'None'"
    },
    "ObservationRuntime": {
      "kind": "class",
      "members": {
        "close": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "from_invocation": {
          "kind": "classmethod",
          "signature": "(cls, invocation: 'ObservationInvocation', *, cancellation_check: 'CancellationCheck | None' = None, heartbeat: 'Heartbeat | None' = None) -> 'ObservationRuntime'"
        },
        "heartbeat": {
          "kind": "method",
          "signature": "(self) -> 'None'"
        },
        "materialize": {
          "kind": "method",
          "signature": "(self, subject: 'ArtifactSubject', *, workspace: 'TransformWorkspace', relative_path: 'str | None' = None, **prepare_kwargs: 'Any') -> 'Path'"
        },
        "open_workspace": {
          "kind": "method",
          "signature": "(self, root: 'Path') -> 'TransformWorkspace'"
        },
        "prepare": {
          "kind": "method",
          "signature": "(self, subjects: 'Sequence[ArtifactSubject] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'"
        },
        "read_bytes": {
          "kind": "method",
          "signature": "(self, subject: 'ArtifactSubject', *, maximum_bytes: 'int', **prepare_kwargs: 'Any') -> 'bytes'"
        },
        "refresh_capability": {
          "kind": "method",
          "signature": "(self, capability_token: 'str') -> 'None'"
        },
        "stream": {
          "kind": "method",
          "signature": "(self, subject: 'ArtifactSubject', *, start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608, **prepare_kwargs: 'Any') -> 'Iterator[Iterator[bytes]]'"
        },
        "subjects": {
          "kind": "method",
          "signature": "(self) -> 'tuple[tuple[ArtifactSubject, ClaimedArtifact], ...]'"
        }
      },
      "signature": "(api: 'Any', *, request: 'ObservationRequest', claim_id: 'str', fence: 'int', cancellation_check: 'CancellationCheck | None' = None, heartbeat: 'Heartbeat | None' = None, workspace_assurance: 'str' = 'ephemeral', owned_api: 'bool' = False) -> 'None'"
    },
    "ObserverClient": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "(self) -> 'ObserverDescriptor'"
        },
        "observe": {
          "kind": "method",
          "signature": "(self, invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'Any'"
        }
      },
      "signature": "(*args, **kwargs)"
    },
    "ObserverConformanceResult": {
      "kind": "class",
      "members": {
        "validate_result": {
          "kind": "method",
          "signature": "(self) -> 'Self'"
        }
      },
      "schema_sha256": "e27ed5abfaeb7544a3910aa4ab0216c2fc7f5fe0341296cb0b299304e601db95",
      "signature": "(*, format: Literal['stove0-observer-conformance-result/v1'] = 'stove0-observer-conformance-result/v1', status: Literal['conformant', 'partially-exercised', 'inspected'], descriptor: stove0_protocol.models.ObserverDescriptor, coverage: stove0_observer_support.conformance.ObserverConformanceCoverage, contracts: tuple[stove0_observer_support.conformance.ObserverContractConformance, ...]) -> None"
    },
    "ObserverHttpBinding": {
      "kind": "class",
      "members": {
        "handle": {
          "kind": "method",
          "signature": "(self, method: 'str', path: 'str', body: 'bytes' = b'') -> 'ObserverHttpResponse'"
        }
      },
      "signature": "(observer: 'ContentObserver', *, semantic_validators: 'SemanticValidatorProvider | None' = None, maximum_request_bytes: 'int' = 4194304, maximum_concurrency: 'int' = 1) -> 'None'"
    },
    "ObserverHttpResponse": {
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
      "signature": "(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None"
    },
    "conformance_report": {
      "kind": "function",
      "signature": "(client: 'ObserverClient', *, invocations: 'Sequence[ObservationInvocation]' = (), semantic_vectors: 'Sequence[SemanticFactsConformanceVectors]' = (), semantic_validators: 'SemanticValidatorProvider | None' = None) -> 'ObserverConformanceResult'"
    },
    "observer_schema_bundle": {
      "kind": "function",
      "signature": "() -> 'dict[str, Any]'"
    }
  },
  "module": "stove0_observer_support"
}
```
