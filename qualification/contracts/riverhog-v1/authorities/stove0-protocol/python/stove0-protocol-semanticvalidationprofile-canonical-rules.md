# stove0_protocol.SemanticValidationProfile.canonical_rules

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-semanticvalidationprofile-272a1a30d6:9cc0143643 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2cd2f6ab6a"></a>
- <a id="s-c3ca216067"></a>`distribution`: `stove0-protocol`
- <a id="s-237970d523"></a>`module`: `stove0_protocol`
- <a id="s-8234643174"></a>`name`: `canonical_rules`
- <a id="s-a812c21bdd"></a>`owner`: `stove0_protocol.SemanticValidationProfile`
- <a id="s-8880fecd7b"></a>`unit`: `member`

### Declared structure

- <a id="s-200fd2ba9c"></a>`kind`: `"classmethod"`
- <a id="s-d09290b5f5"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfile](stove0-protocol-semanticvalidationprofile.md)

## Governing policies

- <a id="pa-a17776fbc7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.SemanticValidationProfile.canonical_rules`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0c9f0e77f1dcce6ecafe06a9ed463d4e3c7d4259a35d5f18ad8736d7d4afd4f7 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_rules",
  "owner": "stove0_protocol.SemanticValidationProfile",
  "unit": "member"
}
```

</details>
