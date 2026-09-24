# stove0_protocol.RecipeIdentityRef.to_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-recipeidentityref-to-identity:aec7313532 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b2255a1978"></a>
- <a id="s-24932ead09"></a>`distribution`: `stove0-protocol`
- <a id="s-3841ceece9"></a>`module`: `stove0_protocol`
- <a id="s-1e55bf2dc2"></a>`name`: `to_identity`
- <a id="s-5667a48f28"></a>`owner`: `stove0_protocol.RecipeIdentityRef`
- <a id="s-7dcf4c4058"></a>`unit`: `member`

### Declared structure

- <a id="s-77edc9a09c"></a>`kind`: `"method"`
- <a id="s-6a9f40c84c"></a>`signature`: `"\"(self) -> 'RecipeIdentity'\""`

## Maintained corroboration

### Related interface records

- [RecipeIdentityRef](stove0-protocol-recipeidentityref.md)

## Governing policies

- <a id="pa-411cf54aff"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.RecipeIdentityRef.to_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af7ee7230e9b6c47f43b3a18bbc619d11a16c70e3becc0d0e8f4456db3facb91 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'RecipeIdentity'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "to_identity",
  "owner": "stove0_protocol.RecipeIdentityRef",
  "unit": "member"
}
```

</details>
