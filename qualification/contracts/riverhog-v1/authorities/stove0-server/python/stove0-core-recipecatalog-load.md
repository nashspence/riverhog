# stove0_core.RecipeCatalog.load

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog-load:e0d289420d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8572a7270e"></a>
- <a id="s-5ef8be3fd9"></a>`distribution`: `stove0-server`
- <a id="s-3820c65545"></a>`module`: `stove0_core`
- <a id="s-7d378cdd6c"></a>`name`: `load`
- <a id="s-f3deb4c75d"></a>`owner`: `stove0_core.RecipeCatalog`
- <a id="s-a793e81490"></a>`unit`: `member`

### Declared structure

- <a id="s-014dd22da3"></a>`kind`: `"classmethod"`
- <a id="s-49b5dd7b1c"></a>`signature`: `"\"(cls, path: 'Path') -> 'RecipeCatalog'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-core-recipecatalog.md)

## Governing policies

- <a id="pa-9613d88add"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog.load`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 002724b0b8904aea1cce33511d34c0aadfdef3788072b73bfc69a940cee38f78 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, path: 'Path') -> 'RecipeCatalog'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load",
  "owner": "stove0_core.RecipeCatalog",
  "unit": "member"
}
```
