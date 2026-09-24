# riverhog_client.processing.CollectionTransformRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-collectiontran-1f02ba7e76:b3a78f2a36 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b5572c6855"></a>
- <a id="s-8120341877"></a>`distribution`: `riverhog-client`
- <a id="s-5bd1a4b840"></a>`module`: `riverhog_client.processing`
- <a id="s-2d48d52c89"></a>`name`: `open_workspace`
- <a id="s-84828ff5ef"></a>`owner`: `riverhog_client.processing.CollectionTransformRuntime`
- <a id="s-cf7137aa51"></a>`unit`: `member`

### Declared structure

- <a id="s-6e0ec0f597"></a>`kind`: `"method"`
- <a id="s-2dbd6f1a17"></a>`signature`: `"\"(self, root: 'Path', *, declared_protection: 'DeclaredWorkspaceProtection') -> 'ProcessingWorkspace'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-processing-collectiontransformruntime.md)

## Governing policies

- <a id="pa-a0293ab24c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.CollectionTransformRuntime.open_workspace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf6dcb1d4d288fb9dd346452b7460a0c0ce52b30e97f8008ae199f8541c15252 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path', *, declared_protection: 'DeclaredWorkspaceProtection') -> 'ProcessingWorkspace'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "open_workspace",
  "owner": "riverhog_client.processing.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
