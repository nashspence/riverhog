# stove0_observer_protocol.SemanticValidationProfile.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidati-87624d3989:801d56219f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cab33458a2"></a>
- <a id="s-c511a2f0ff"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-1b994e436e"></a>`module`: `stove0_observer_protocol`
- <a id="s-de5385d440"></a>`name`: `verify_digest`
- <a id="s-803d78923a"></a>`owner`: `stove0_observer_protocol.SemanticValidationProfile`
- <a id="s-8e17383cd6"></a>`unit`: `member`

### Declared structure

- <a id="s-45e840d502"></a>`kind`: `"method"`
- <a id="s-094a38c800"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfile](stove0-observer-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-edd0b312f4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidationProfile.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd737a2bfed8e5e6b2a4e695fdb2f9a4f9227499fd469787b4f46e9316a189f6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "verify_digest",
  "owner": "stove0_observer_protocol.SemanticValidationProfile",
  "unit": "member"
}
```

</details>
