# stove0_protocol.JoinDeclaration.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-joindeclaration-seal:80d59d8cde -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-24e9421ae7"></a>
- <a id="s-e3339b742d"></a>`distribution`: `stove0-protocol`
- <a id="s-36753b7263"></a>`module`: `stove0_protocol`
- <a id="s-a0418b88b9"></a>`name`: `seal`
- <a id="s-a9d38a6a17"></a>`owner`: `stove0_protocol.JoinDeclaration`
- <a id="s-0f6d0e09f0"></a>`unit`: `member`

### Declared structure

- <a id="s-d21b17df63"></a>`kind`: `"classmethod"`
- <a id="s-781c0afd9e"></a>`signature`: `"\"(cls, *, members: 'Sequence[JoinMemberDeclaration]', recipe: 'RecipeIdentityRef', effective_intent: 'Mapping[str, JsonValue]', workflow_intent: 'WorkflowPlanIntent') -> 'JoinDeclaration'\""`

## Maintained corroboration

### Related interface records

- [JoinDeclaration](stove0-protocol-joindeclaration.md)

## Governing policies

- <a id="pa-6e3c2f1118"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JoinDeclaration.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c79b13f2c71d0dd1e011c6bbbd46c91632d751f5be8da92a886935646be78645 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, members: 'Sequence[JoinMemberDeclaration]', recipe: 'RecipeIdentityRef', effective_intent: 'Mapping[str, JsonValue]', workflow_intent: 'WorkflowPlanIntent') -> 'JoinDeclaration'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.JoinDeclaration",
  "unit": "member"
}
```

</details>
