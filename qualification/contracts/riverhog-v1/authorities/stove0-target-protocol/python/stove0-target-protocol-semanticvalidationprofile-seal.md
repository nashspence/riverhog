# stove0_target_protocol.SemanticValidationProfile.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticvalidation-f54ea638db:b547477cea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e3a2d0f57"></a>
- <a id="s-a5a39c270d"></a>`distribution`: `stove0-target-protocol`
- <a id="s-1a298a61a8"></a>`module`: `stove0_target_protocol`
- <a id="s-17b4647b08"></a>`name`: `seal`
- <a id="s-af9915ec6a"></a>`owner`: `stove0_target_protocol.SemanticValidationProfile`
- <a id="s-9fcfd9cd12"></a>`unit`: `member`

### Declared structure

- <a id="s-010a165881"></a>`kind`: `"classmethod"`
- <a id="s-a2977713c9"></a>`signature`: `"\"(cls, payload: 'SemanticValidationProfilePayload') -> 'SemanticValidationProfile'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfile](stove0-target-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-69edb33ff3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticValidationProfile.seal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35e2893ce21fdcb1c9e4197d37f78ab7f561828300be840c1e2884bdfa2cfcae -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SemanticValidationProfilePayload') -> 'SemanticValidationProfile'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.SemanticValidationProfile",
  "unit": "member"
}
```
