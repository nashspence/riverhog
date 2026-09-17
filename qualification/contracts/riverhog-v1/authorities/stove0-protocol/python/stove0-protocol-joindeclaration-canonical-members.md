# stove0_protocol.JoinDeclaration.canonical_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joindeclaration-canonical-members:12fdcc3e82 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6591fd49b"></a>
- <a id="s-5cb675ba6a"></a>`distribution`: `stove0-protocol`
- <a id="s-a8a8a5e208"></a>`module`: `stove0_protocol`
- <a id="s-3a2aea6870"></a>`name`: `canonical_members`
- <a id="s-803e4bac8f"></a>`owner`: `stove0_protocol.JoinDeclaration`
- <a id="s-e0f1a2e122"></a>`unit`: `member`

### Declared structure

- <a id="s-a9a4769a6c"></a>`kind`: `"classmethod"`
- <a id="s-cb034319f8"></a>`signature`: `"\"(cls, value: 'tuple[JoinMemberDeclaration, ...]') -> 'tuple[JoinMemberDeclaration, ...]'\""`

## Maintained corroboration

### Related interface records

- [JoinDeclaration](stove0-protocol-joindeclaration.md)

## Governing policies

- <a id="pa-e3f6aa5bde"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinDeclaration.canonical_members`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 50234fef907dedb4c12e3bb9eb3332d4a0a01025177c650f4fb221da138aa2cb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[JoinMemberDeclaration, ...]') -> 'tuple[JoinMemberDeclaration, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_members",
  "owner": "stove0_protocol.JoinDeclaration",
  "unit": "member"
}
```

</details>
