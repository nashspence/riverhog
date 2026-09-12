# stove0_observer_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client:61e01242bf -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-client` |
| Interface | `python` |
| Family | `modules` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/python/16`

## Effective policies

- `compatibility/python-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `python:stove0-observer-client` — `reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py::<module>`
- Proof: `make dist-smoke`
- Proof: `make build`

## Contract

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
