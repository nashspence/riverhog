# stove0_core.RecipeCatalog.valid_catalog

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog-valid-catalog:beccb8a217 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-96e60314cc"></a>
- <a id="s-ffceabbd66"></a>`distribution`: `stove0-server`
- <a id="s-ff1706132b"></a>`module`: `stove0_core`
- <a id="s-33488814fe"></a>`name`: `valid_catalog`
- <a id="s-d78e345779"></a>`owner`: `stove0_core.RecipeCatalog`
- <a id="s-06b40fe1ce"></a>`unit`: `member`

### Declared structure

- <a id="s-86fb53384b"></a>`kind`: `"method"`
- <a id="s-9235c46caa"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-core-recipecatalog.md)

## Governing policies

- <a id="pa-9b809b6fcd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog.valid_catalog`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 052a51c561cbae836fae8e320fbf947e4b5dde55cd9488c0ea45b6f9e5ea92e7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "valid_catalog",
  "owner": "stove0_core.RecipeCatalog",
  "unit": "member"
}
```

</details>
