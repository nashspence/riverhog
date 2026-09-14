# riverhog_client.transform.CollectionTransformRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-008a2f9d7e:a049bd3ce4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f3f4992e68"></a>
- <a id="s-152ec01be8"></a>`distribution`: `riverhog-client`
- <a id="s-4b0448b60b"></a>`module`: `riverhog_client.transform`
- <a id="s-ad44ec3a59"></a>`name`: `open_workspace`
- <a id="s-f3f2f66239"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-d2dcb4a1f9"></a>`unit`: `member`

### Declared structure

- <a id="s-41beee1ad7"></a>`kind`: `"method"`
- <a id="s-77f654d393"></a>`signature`: `"\"(self, root: 'Path', *, assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-f7d9a6324e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.open_workspace`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9069b927904938e2cf7a44258ebc58bcbc8e7dc54c69037a6b7f857c9dfba554 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path', *, assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "open_workspace",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```
