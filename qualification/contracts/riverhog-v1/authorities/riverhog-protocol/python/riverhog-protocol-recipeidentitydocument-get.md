# riverhog_protocol.RecipeIdentityDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-recipeidentitydocument-get:d254e4780e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-009df1a3ea"></a>
- <a id="s-b0b1ba8c9b"></a>`distribution`: `riverhog-protocol`
- <a id="s-11e622169b"></a>`module`: `riverhog_protocol`
- <a id="s-66aa06969c"></a>`name`: `get`
- <a id="s-6a577e0073"></a>`owner`: `riverhog_protocol.RecipeIdentityDocument`
- <a id="s-95ec24c14e"></a>`unit`: `member`

### Declared structure

- <a id="s-1e6bc7a9db"></a>`kind`: `"method"`
- <a id="s-acfd45089a"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [RecipeIdentityDocument](riverhog-protocol-recipeidentitydocument.md)

## Governing policies

- <a id="pa-e88a042ef2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.RecipeIdentityDocument.get`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e08e8ab55d2feca0dbd5ef9ef3fe321164fcd3c018d6a807dea34d1611e965b2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.RecipeIdentityDocument",
  "unit": "member"
}
```
