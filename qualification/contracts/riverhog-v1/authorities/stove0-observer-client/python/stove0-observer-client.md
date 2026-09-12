# stove0_observer_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client:61e01242bf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [python](index.md) |
| Family | [modules](index.md#f-862b3dda9b77) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d78d1a03651f"></a>
| Field | Shape |
|---|---|
| <a id="s-2679979a1ace"></a>`distribution` | "stove0-observer-client" |
| <a id="s-55ec31989125"></a>`exports` | additional keys=`ContentObserverClient`, `ObserverProtocolError`, `SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP`, `load_semantic_validator_registry` |
| <a id="s-23239c93e3cf"></a>`module` | "stove0_observer_client" |

## Governing policies

- <a id="pa-ed228d0b3fb7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba506)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-client](../../../evidence/sources.md#src-657eb556bb16) — `reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py::<module>`

### Machine authority

- `/external_contract/python/16`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ec0ff12bc6cc427bf8926069b83f89fb77a3da3ee89eaf858e7afaac1ec6b17 -->

```json
{
  "distribution": "stove0-observer-client",
  "exports": {
    "ContentObserverClient": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "(self) -> 'ObserverDescriptor'"
        },
        "observe": {
          "kind": "method",
          "signature": "(self, invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ObservationResult'"
        }
      },
      "signature": "(base_url: 'str', *, token: 'str | None' = None, timeout: 'float | None' = 300.0, allow_insecure_http: 'bool' = False, semantic_validators: 'SemanticValidatorProvider | None' = None) -> 'None'"
    },
    "ObserverProtocolError": {
      "kind": "class",
      "signature": "(message: 'str', *, failure_kind: \"Literal['remote_rejection', 'transport', 'invalid_response', 'unsupported_semantics']\", code: 'str | None' = None, observed_status: 'int | None' = None, details: 'Mapping[str, Any] | None' = None) -> 'None'"
    },
    "SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP": {
      "kind": "constant",
      "value": "stove0.observer-semantic-validators"
    },
    "load_semantic_validator_registry": {
      "kind": "function",
      "signature": "(provider_names: 'Sequence[str]') -> 'SemanticValidatorRegistry'"
    }
  },
  "module": "stove0_observer_client"
}
```
