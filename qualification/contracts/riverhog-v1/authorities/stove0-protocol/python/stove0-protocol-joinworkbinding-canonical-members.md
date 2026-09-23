# stove0_protocol.JoinWorkBinding.canonical_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joinworkbinding-canonical-members:d9cf172172 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-436ba14a71"></a>
- <a id="s-211cde19c1"></a>`distribution`: `stove0-protocol`
- <a id="s-96e2a30ec8"></a>`module`: `stove0_protocol`
- <a id="s-6e9e39a81c"></a>`name`: `canonical_members`
- <a id="s-5806442fd4"></a>`owner`: `stove0_protocol.JoinWorkBinding`
- <a id="s-f83c6b3a48"></a>`unit`: `member`

### Declared structure

- <a id="s-bddeed0eb9"></a>`kind`: `"classmethod"`
- <a id="s-7d5efc95de"></a>`signature`: `"\"(cls, value: 'tuple[JoinWorkMemberBinding, ...]') -> 'tuple[JoinWorkMemberBinding, ...]'\""`

## Maintained corroboration

### Related interface records

- [JoinWorkBinding](stove0-protocol-joinworkbinding.md)

## Governing policies

- <a id="pa-817d9d4cb5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinWorkBinding.canonical_members`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ce8e35029b80f7863c5545f1fa740986494385a6ea125bccc0fa9d13c4bde4d1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[JoinWorkMemberBinding, ...]') -> 'tuple[JoinWorkMemberBinding, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_members",
  "owner": "stove0_protocol.JoinWorkBinding",
  "unit": "member"
}
```

</details>
