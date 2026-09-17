# stove0_api.create_app

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-api-create-app:91d139492e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-159448df0c"></a>
- <a id="s-76bffcb772"></a>`distribution`: `stove0-server`
- <a id="s-affdfd0d22"></a>`module`: `stove0_api`
- <a id="s-6b8bb645b7"></a>`name`: `create_app`
- <a id="s-4b7a072e69"></a>`unit`: `export`

### Declared structure

- <a id="s-2c06a26736"></a>`kind`: `"function"`
- <a id="s-47716a775d"></a>`signature`: `"\"(composition: 'Stove0Composition') -> 'FastAPI'\""`

## Governing policies

- <a id="pa-0655faf22f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_api](../../../evidence/sources.md#src-d5a12e8c56) — [reference/stove0/application/server/src/stove0\_api/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_api/__init__.py)

### Machine authority

- `/external_contract/python/stove0_api.create_app`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bb9fe9967abd1bcf62c7094f3d406ec37976e52aa18a01e6e73a15ddf5bb55c4 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(composition: 'Stove0Composition') -> 'FastAPI'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_api",
  "name": "create_app",
  "unit": "export"
}
```

</details>
