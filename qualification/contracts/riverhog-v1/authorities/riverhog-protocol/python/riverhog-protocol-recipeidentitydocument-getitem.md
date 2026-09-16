# riverhog_protocol.RecipeIdentityDocument.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentitydocument-getitem:e34757ee5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-771cbb2d32"></a>
- <a id="s-7265056e5d"></a>`distribution`: `riverhog-protocol`
- <a id="s-f93ff95d53"></a>`module`: `riverhog_protocol`
- <a id="s-18406639f3"></a>`name`: `__getitem__`
- <a id="s-e9a176ab16"></a>`owner`: `riverhog_protocol.RecipeIdentityDocument`
- <a id="s-f4c56a5fb3"></a>`unit`: `member`

### Declared structure

- <a id="s-317b3007e9"></a>`kind`: `"method"`
- <a id="s-6ec4b8ef8c"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [RecipeIdentityDocument](riverhog-protocol-recipeidentitydocument.md)

## Governing policies

- <a id="pa-5105eecbf8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentityDocument.__getitem__`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab5ad1beb29a37ba3c00e146385682ae248ca69090f9307f639b28e71d48b184 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "__getitem__",
  "owner": "riverhog_protocol.RecipeIdentityDocument",
  "unit": "member"
}
```
