# stove0_target_protocol.SemanticValidationProfile.canonical_rules

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-semanticvalidation-45ebeae864:5919c21384 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9a95dec16a"></a>
- <a id="s-d1a7ec138f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-929107b226"></a>`module`: `stove0_target_protocol`
- <a id="s-6cbb9a7547"></a>`name`: `canonical_rules`
- <a id="s-43d772d2ba"></a>`owner`: `stove0_target_protocol.SemanticValidationProfile`
- <a id="s-dfc355c5a7"></a>`unit`: `member`

### Declared structure

- <a id="s-fb8c838103"></a>`kind`: `"classmethod"`
- <a id="s-78ef900a20"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfile](stove0-target-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-8a70418595"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.SemanticValidationProfile.canonical_rules`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af6cc384ce9e5c97ca6cb5513d0a58cb14fa651698d6b644039f14e142f2ea18 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_rules",
  "owner": "stove0_target_protocol.SemanticValidationProfile",
  "unit": "member"
}
```

</details>
