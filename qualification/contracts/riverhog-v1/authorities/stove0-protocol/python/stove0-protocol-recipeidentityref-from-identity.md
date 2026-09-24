# stove0_protocol.RecipeIdentityRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-recipeidentityref-from-identity:12339a564b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0eebfc0a30"></a>
- <a id="s-9542b4fcc4"></a>`distribution`: `stove0-protocol`
- <a id="s-865d08dbbd"></a>`module`: `stove0_protocol`
- <a id="s-48e93b2e5c"></a>`name`: `from_identity`
- <a id="s-b90e326e96"></a>`owner`: `stove0_protocol.RecipeIdentityRef`
- <a id="s-bf8e587ca1"></a>`unit`: `member`

### Declared structure

- <a id="s-4219ed0dc5"></a>`kind`: `"classmethod"`
- <a id="s-eb82b793b1"></a>`signature`: `"\"(cls, value: 'RecipeIdentity') -> 'RecipeIdentityRef'\""`

## Maintained corroboration

### Related interface records

- [RecipeIdentityRef](stove0-protocol-recipeidentityref.md)

## Governing policies

- <a id="pa-087f4dbfe9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.RecipeIdentityRef.from_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f36ff2f837de7ea1f341edbc74294dc08a461489c36fc4c3413bd7b9913c1006 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'RecipeIdentity') -> 'RecipeIdentityRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_identity",
  "owner": "stove0_protocol.RecipeIdentityRef",
  "unit": "member"
}
```

</details>
