# stove0_observer_client

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client:e8a6ea21c4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [Python](index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-1e68d54c68"></a>
| Field | Shape |
|---|---|
| <a id="s-1139096207"></a>`candidate_id` | "python:stove0-observer-client:stove0_observer_client" |
| <a id="s-fd72a01e6f"></a>`distribution` | "stove0-observer-client" |
| <a id="s-4ae6966c67"></a>`exports` | additional keys=`ContentObserverClient`, `ObserverProtocolError`, `SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP`, `load_semantic_validator_registry` |
| <a id="s-064862b731"></a>`module` | "stove0_observer_client" |

## Governing policies

- <a id="pa-7b139d83ec"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-client:stove0_observer_client](../../../evidence/sources.md#src-67dbe161ba) — `reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py`

### Machine authority

- `/external_contract/python/41`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 435e973ef478ceb0258c481e4d52308111bb0b1c230d67455fbd510b296309a0 -->

```json
{
  "candidate_id": "python:stove0-observer-client:stove0_observer_client",
  "distribution": "stove0-observer-client",
  "exports": {
    "ContentObserverClient": {
      "kind": "class",
      "members": {
        "descriptor": {
          "kind": "method",
          "signature": "\"(self) -> 'ObserverDescriptor'\""
        },
        "observe": {
          "kind": "method",
          "signature": "\"(self, invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'ObservationResult'\""
        }
      },
      "signature": "\"(base_url: 'str', *, token: 'str | None' = None, timeout: 'float | None' = 300.0, allow_insecure_http: 'bool' = False, semantic_validators: 'SemanticValidatorProvider | None' = None) -> 'None'\""
    },
    "ObserverProtocolError": {
      "kind": "class",
      "signature": "'(message: \\'str\\', *, failure_kind: \"Literal[\\'remote_rejection\\', \\'transport\\', \\'invalid_response\\', \\'unsupported_semantics\\']\", code: \\'str | None\\' = None, observed_status: \\'int | None\\' = None, details: \\'Mapping[str, Any] | None\\' = None) -> \\'None\\''"
    },
    "SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP": {
      "kind": "constant",
      "value": "stove0.observer-semantic-validators"
    },
    "load_semantic_validator_registry": {
      "kind": "function",
      "signature": "\"(provider_names: 'Sequence[str]') -> 'SemanticValidatorRegistry'\""
    }
  },
  "module": "stove0_observer_client"
}
```
