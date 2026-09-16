# stove0_core.RecipeCatalog.validation_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog-validation-document:7fd1b472ea -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff99562707"></a>
- <a id="s-a964f22065"></a>`distribution`: `stove0-server`
- <a id="s-db2c4cffc8"></a>`module`: `stove0_core`
- <a id="s-71ca884a3d"></a>`name`: `validation_document`
- <a id="s-73f631f485"></a>`owner`: `stove0_core.RecipeCatalog`
- <a id="s-9b41b8de20"></a>`unit`: `member`

### Declared structure

- <a id="s-8934e91888"></a>`kind`: `"method"`
- <a id="s-725dded9e2"></a>`signature`: `"\"(self) -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-core-recipecatalog.md)

## Governing policies

- <a id="pa-79e9c7fc00"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog.validation_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 03579b9a65d49c7f0bbb15e4b8f179268ab24af776a8ff61a9ca8b6c88708b66 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "validation_document",
  "owner": "stove0_core.RecipeCatalog",
  "unit": "member"
}
```

</details>
