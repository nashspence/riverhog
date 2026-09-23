# stove0_protocol.RecipeRef.from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-reciperef-from-identity:8eb1b324f2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7bd33e3be"></a>
- <a id="s-84e278d673"></a>`distribution`: `stove0-protocol`
- <a id="s-211fc8a3ca"></a>`module`: `stove0_protocol`
- <a id="s-a4c13d90c8"></a>`name`: `from_identity`
- <a id="s-aad23407ee"></a>`owner`: `stove0_protocol.RecipeRef`
- <a id="s-79a7bc1117"></a>`unit`: `member`

### Declared structure

- <a id="s-99e19b1bd7"></a>`kind`: `"classmethod"`
- <a id="s-e5f225be7b"></a>`signature`: `"\"(cls, value: 'RecipeIdentity') -> 'RecipeRef'\""`

## Maintained corroboration

### Related interface records

- [RecipeRef](stove0-protocol-reciperef.md)

## Governing policies

- <a id="pa-d09d3362de"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.RecipeRef.from_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12d18d12e02c28a9d4dd618fd8bc64ea011f86f5e39e63fb309c7b39b724189f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'RecipeIdentity') -> 'RecipeRef'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "from_identity",
  "owner": "stove0_protocol.RecipeRef",
  "unit": "member"
}
```

</details>
