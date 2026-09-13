# stove0_observer_support.ObserverClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerclient-descriptor:f9462d5ef0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb5c62074a"></a>
| Field | Shape |
|---|---|
| <a id="s-fea689998b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-525ff1130e"></a>`distribution` | "stove0-observer-support" |
| <a id="s-338db3a644"></a>`module` | "stove0_observer_support" |
| <a id="s-9fb8b31466"></a>`name` | "descriptor" |
| <a id="s-a073f25329"></a>`owner` | "stove0_observer_support.ObserverClient" |
| <a id="s-6751da4d9d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ObserverClient](stove0-observer-support-observerclient.md)

## Governing policies

- <a id="pa-3adb5de7e3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverClient.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89634928a9da22297cf38450a8dcf4ed2dee69ee34d88de9c0259d0e85bcde1b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'ObserverDescriptor'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "descriptor",
  "owner": "stove0_observer_support.ObserverClient",
  "unit": "member"
}
```
