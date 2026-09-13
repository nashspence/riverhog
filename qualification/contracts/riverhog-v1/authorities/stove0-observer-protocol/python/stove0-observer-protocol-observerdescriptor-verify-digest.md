# stove0_observer_protocol.ObserverDescriptor.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observerdescript-5d22d38eaa:f2c594fe60 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-700f344037"></a>
| Field | Shape |
|---|---|
| <a id="s-3e47af6297"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-73a8faf971"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-8a78210026"></a>`module` | "stove0_observer_protocol" |
| <a id="s-675db5a3e5"></a>`name` | "verify_digest" |
| <a id="s-f8267f45b0"></a>`owner` | "stove0_observer_protocol.ObserverDescriptor" |
| <a id="s-8bb1f513e3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObserverDescriptor](stove0-observer-protocol-observerdescriptor.md)

## Governing policies

- <a id="pa-fe652a18db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObserverDescriptor.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7cc4980dbaf17d0b69d41f1e2441b5c1916cec92b208d50c4fe3f3dddc6327d1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.ObserverDescriptor",
  "unit": "member"
}
```
