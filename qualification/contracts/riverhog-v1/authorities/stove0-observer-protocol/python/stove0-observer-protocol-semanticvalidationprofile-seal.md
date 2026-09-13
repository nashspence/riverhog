# stove0_observer_protocol.SemanticValidationProfile.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidati-f5f1daea00:3877516a39 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c974550adb"></a>
| Field | Shape |
|---|---|
| <a id="s-d718fe7f9a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e3f1c516bf"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-d6f9203613"></a>`module` | "stove0_observer_protocol" |
| <a id="s-5a551cf171"></a>`name` | "seal" |
| <a id="s-852e773e11"></a>`owner` | "stove0_observer_protocol.SemanticValidationProfile" |
| <a id="s-aa0a2fb67f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticValidationProfile](stove0-observer-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-c22f2b2b4b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidationProfile.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5eb185ea29a45131dfee075b392a58379160ca9ac9a6e7926deb0fd7aa6c2bf0 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SemanticValidationProfilePayload') -> 'SemanticValidationProfile'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "seal",
  "owner": "stove0_observer_protocol.SemanticValidationProfile",
  "unit": "member"
}
```
