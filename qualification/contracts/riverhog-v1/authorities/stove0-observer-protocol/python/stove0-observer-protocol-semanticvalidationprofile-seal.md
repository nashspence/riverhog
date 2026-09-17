# stove0_observer_protocol.SemanticValidationProfile.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidati-f5f1daea00:3877516a39 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c974550adb"></a>
- <a id="s-e3f1c516bf"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-d6f9203613"></a>`module`: `stove0_observer_protocol`
- <a id="s-5a551cf171"></a>`name`: `seal`
- <a id="s-852e773e11"></a>`owner`: `stove0_observer_protocol.SemanticValidationProfile`
- <a id="s-aa0a2fb67f"></a>`unit`: `member`

### Declared structure

- <a id="s-1b2d75f039"></a>`kind`: `"classmethod"`
- <a id="s-a0739bc576"></a>`signature`: `"\"(cls, payload: 'SemanticValidationProfilePayload') -> 'SemanticValidationProfile'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfile](stove0-observer-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-c22f2b2b4b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidationProfile.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
