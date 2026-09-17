# riverhog_protocol.RecipeIdentity.as_dict

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentity-as-dict:cb1047368c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df7a2821ed"></a>
- <a id="s-e22374fd1d"></a>`distribution`: `riverhog-protocol`
- <a id="s-d5dff03e49"></a>`module`: `riverhog_protocol`
- <a id="s-7093ab5b5a"></a>`name`: `as_dict`
- <a id="s-900dc9b618"></a>`owner`: `riverhog_protocol.RecipeIdentity`
- <a id="s-e1b853cf56"></a>`unit`: `member`

### Declared structure

- <a id="s-07638b3029"></a>`kind`: `"method"`
- <a id="s-2e12b10db0"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [RecipeIdentity](riverhog-protocol-recipeidentity.md)

## Governing policies

- <a id="pa-d2a7c10247"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentity.as_dict`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 65408aeb4318b55740f376204584cda6e2ae959d23d9a09876f24a1465deb491 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "as_dict",
  "owner": "riverhog_protocol.RecipeIdentity",
  "unit": "member"
}
```

</details>
