# riverhog_protocol.RecipeIdentity.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentity-from-mapping:8ad39626fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-87f6581e87"></a>
- <a id="s-544b82db1e"></a>`distribution`: `riverhog-protocol`
- <a id="s-061a92e0b4"></a>`module`: `riverhog_protocol`
- <a id="s-a444198b90"></a>`name`: `from_mapping`
- <a id="s-8166e68d95"></a>`owner`: `riverhog_protocol.RecipeIdentity`
- <a id="s-14dd58b0c5"></a>`unit`: `member`

### Declared structure

- <a id="s-2c3e268ee1"></a>`kind`: `"classmethod"`
- <a id="s-82162f1549"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'RecipeIdentity'\""`

## Maintained corroboration

### Related interface records

- [RecipeIdentity](riverhog-protocol-recipeidentity.md)

## Governing policies

- <a id="pa-3a04ca7ba3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentity.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 84519497ce76264e4e29e63974da2d80818d6813295480880f9459b85998d5db -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'RecipeIdentity'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.RecipeIdentity",
  "unit": "member"
}
```

</details>
