# stove0_observer_protocol.SemanticValidatorBinding.from_profile

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidato-27fe5e4239:94fc7d6ed1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-61c4f50d3b"></a>
- <a id="s-9eed603993"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-f167c0f1a1"></a>`module`: `stove0_observer_protocol`
- <a id="s-4539ab8397"></a>`name`: `from_profile`
- <a id="s-efd059d231"></a>`owner`: `stove0_observer_protocol.SemanticValidatorBinding`
- <a id="s-a0dc5706e2"></a>`unit`: `member`

### Declared structure

- <a id="s-00c2b2372f"></a>`kind`: `"classmethod"`
- <a id="s-0b7551e907"></a>`signature`: `"\"(cls, profile: 'SemanticValidationProfile', validator: 'FactsSemanticValidator') -> 'SemanticValidatorBinding'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidatorBinding](stove0-observer-protocol-semanticvalidatorbinding.md)

## Governing policies

- <a id="pa-e21e688fce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidatorBinding.from_profile`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e6ac7c76db0b34a6e3e01ea2796864806030949e001119d5d3fbabdf88f0adf -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, profile: 'SemanticValidationProfile', validator: 'FactsSemanticValidator') -> 'SemanticValidatorBinding'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "from_profile",
  "owner": "stove0_observer_protocol.SemanticValidatorBinding",
  "unit": "member"
}
```

</details>
