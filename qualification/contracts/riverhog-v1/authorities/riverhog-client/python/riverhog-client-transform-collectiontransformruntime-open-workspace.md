# riverhog_client.transform.CollectionTransformRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-008a2f9d7e:a049bd3ce4 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-77f654d393"></a>`signature`: `"\"(self, root: 'Path', *, declared_protection: 'DeclaredWorkspaceProtection') -> 'TransformWorkspace'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-f7d9a6324e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.open_workspace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 160bae9578ee71564022369413f5230e6e5d86fe720473f3ef7470e16fd1db22 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path', *, declared_protection: 'DeclaredWorkspaceProtection') -> 'TransformWorkspace'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "open_workspace",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
