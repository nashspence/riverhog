# stove0_observer_support.ObserverClient.observe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerclient-observe:5b8ac37d95 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f7075be531"></a>
| Field | Shape |
|---|---|
| <a id="s-a92c0b8ca9"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9ff899cf07"></a>`distribution` | "stove0-observer-support" |
| <a id="s-e5937678e2"></a>`module` | "stove0_observer_support" |
| <a id="s-0661dda79f"></a>`name` | "observe" |
| <a id="s-5f58c3fb83"></a>`owner` | "stove0_observer_support.ObserverClient" |
| <a id="s-799e62272f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ObserverClient](stove0-observer-support-observerclient.md)

## Governing policies

- <a id="pa-77b147e683"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverClient.observe`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8524b1a72bfaa5b14c6ed2057a797bc4028553a35e87ed059182fc01326fd461 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, invocation: 'ObservationInvocation', *, descriptor: 'ObserverDescriptor') -> 'Any'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "observe",
  "owner": "stove0_observer_support.ObserverClient",
  "unit": "member"
}
```
