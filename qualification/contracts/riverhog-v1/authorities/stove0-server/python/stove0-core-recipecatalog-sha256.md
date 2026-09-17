# stove0_core.RecipeCatalog.sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipecatalog-sha256:ab469cb347 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2590f02a7"></a>
- <a id="s-12839f87a4"></a>`distribution`: `stove0-server`
- <a id="s-04e5afc556"></a>`module`: `stove0_core`
- <a id="s-c77381d35b"></a>`name`: `sha256`
- <a id="s-715f3b1ab7"></a>`owner`: `stove0_core.RecipeCatalog`
- <a id="s-cd012554b3"></a>`unit`: `member`

### Declared structure

- <a id="s-9ff6d682db"></a>`kind`: `"property"`
- <a id="s-1549958c4f"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [RecipeCatalog](stove0-core-recipecatalog.md)

## Governing policies

- <a id="pa-6f9ac0dda3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipeCatalog.sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5d89ef12ba0be54568e4d1c23d358c441df3e181843aade02980fd6a8707f80 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "sha256",
  "owner": "stove0_core.RecipeCatalog",
  "unit": "member"
}
```

</details>
