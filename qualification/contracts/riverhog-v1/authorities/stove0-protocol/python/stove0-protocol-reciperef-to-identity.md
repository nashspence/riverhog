# stove0_protocol.RecipeRef.to_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-reciperef-to-identity:6d7e036c85 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b7787332bb"></a>
- <a id="s-d77d473d1c"></a>`distribution`: `stove0-protocol`
- <a id="s-943f1011ad"></a>`module`: `stove0_protocol`
- <a id="s-9e0ecc3d2a"></a>`name`: `to_identity`
- <a id="s-d7a8037dd0"></a>`owner`: `stove0_protocol.RecipeRef`
- <a id="s-87b5603ca5"></a>`unit`: `member`

### Declared structure

- <a id="s-0fa5d864ad"></a>`kind`: `"method"`
- <a id="s-302b94a1f6"></a>`signature`: `"\"(self) -> 'RecipeIdentity'\""`

## Maintained corroboration

### Related interface records

- [RecipeRef](stove0-protocol-reciperef.md)

## Governing policies

- <a id="pa-e878d20981"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.RecipeRef.to_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 686d8fd803c7f599bdc45c4d8bc64e25525822af355fcbdeea330cd03111df29 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'RecipeIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "to_identity",
  "owner": "stove0_protocol.RecipeRef",
  "unit": "member"
}
```

</details>
