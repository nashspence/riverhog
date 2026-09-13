# stove0_observer_support.ObserverHttpBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerhttpbinding:00e96a190a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-26ac15e823"></a>
| Field | Shape |
|---|---|
| <a id="s-c92255b94e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a26a054210"></a>`distribution` | "stove0-observer-support" |
| <a id="s-56407a7a54"></a>`module` | "stove0_observer_support" |
| <a id="s-dffd5250a5"></a>`name` | "ObserverHttpBinding" |
| <a id="s-df44d8f57c"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ObserverHttpBinding.handle](stove0-observer-support-observerhttpbinding-handle.md)

## Governing policies

- <a id="pa-ecc3737af4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverHttpBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c1f5929b8ce3e924f0a274f245ddef426e9698f7fa865f228a4089f98911bda -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(observer: 'ContentObserver', *, semantic_validators: 'SemanticValidatorProvider | None' = None, maximum_request_bytes: 'int' = 4194304, maximum_concurrency: 'int' = 1) -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObserverHttpBinding",
  "unit": "export"
}
```
