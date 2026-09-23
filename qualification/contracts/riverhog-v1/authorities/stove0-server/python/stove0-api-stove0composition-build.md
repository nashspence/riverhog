# stove0_api.Stove0Composition.build

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-api-stove0composition-build:ea842c8227 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e2853c5ac"></a>
- <a id="s-b97387d2d2"></a>`distribution`: `stove0-server`
- <a id="s-933842738e"></a>`module`: `stove0_api`
- <a id="s-7e326e0257"></a>`name`: `build`
- <a id="s-2492fbc31e"></a>`owner`: `stove0_api.Stove0Composition`
- <a id="s-479fd5d756"></a>`unit`: `member`

### Declared structure

- <a id="s-afaa762809"></a>`kind`: `"classmethod"`
- <a id="s-460c36d0ee"></a>`signature`: `"\"(cls, config: 'Stove0RuntimeConfig') -> 'Stove0Composition'\""`

## Maintained corroboration

### Related interface records

- [Stove0Composition](stove0-api-stove0composition.md)

## Governing policies

- <a id="pa-96bf0557a4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_api](../../../evidence/sources/authorities.md#src-d5a12e8c56) — [some-implementations/stove0/application/server/src/stove0\_api/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_api/__init__.py)

### Machine authority

- `/external_contract/python/stove0_api.Stove0Composition.build`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1e8c518d6c8299284bed5251128f2260aad8aa2d8e9f38ca052944c224c2c6f1 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, config: 'Stove0RuntimeConfig') -> 'Stove0Composition'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_api",
  "name": "build",
  "owner": "stove0_api.Stove0Composition",
  "unit": "member"
}
```

</details>
