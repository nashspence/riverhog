# stove0_protocol.SemanticValidationProfilePayload.canonical_rules

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-semanticvalidationprofile-9bdd9f0724:3f6a2aad77 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9ade70c0e"></a>
- <a id="s-809435839d"></a>`distribution`: `stove0-protocol`
- <a id="s-fe81d556f4"></a>`module`: `stove0_protocol`
- <a id="s-ceeba6a009"></a>`name`: `canonical_rules`
- <a id="s-2f40a1b905"></a>`owner`: `stove0_protocol.SemanticValidationProfilePayload`
- <a id="s-22498b23fe"></a>`unit`: `member`

### Declared structure

- <a id="s-1ced120a0c"></a>`kind`: `"classmethod"`
- <a id="s-aaab2b9ba2"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [SemanticValidationProfilePayload](stove0-protocol-semanticvalidationprofilepayload.md)

## Governing policies

- <a id="pa-4ec368b50e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.SemanticValidationProfilePayload.canonical_rules`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 064ff8d6a9429295576d5a18f73b186d1ffa0ddf4af389ec6a8d72f78db91317 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_rules",
  "owner": "stove0_protocol.SemanticValidationProfilePayload",
  "unit": "member"
}
```

</details>
