# riverhog_client.processing.ClaimedCollectionRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-7e0347cc87:54a879c3e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-622f96210f"></a>
- <a id="s-ad6067bc8d"></a>`distribution`: `riverhog-client`
- <a id="s-8121f5552a"></a>`module`: `riverhog_client.processing`
- <a id="s-7fc451f1d4"></a>`name`: `open_workspace`
- <a id="s-dcd9f0918b"></a>`owner`: `riverhog_client.processing.ClaimedCollectionRuntime`
- <a id="s-fbd0e2b6dc"></a>`unit`: `member`

### Declared structure

- <a id="s-574875b7d4"></a>`kind`: `"method"`
- <a id="s-529bb0caeb"></a>`signature`: `"\"(self, root: 'Path', *, declared_protection: 'DeclaredWorkspaceProtection') -> 'ProcessingWorkspace'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-processing-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-416f4856b8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionRuntime.open_workspace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08763b1e287c82bdda57d2aad7472a65d56ff21dae7b604e88f8a3b7b86362d6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path', *, declared_protection: 'DeclaredWorkspaceProtection') -> 'ProcessingWorkspace'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "open_workspace",
  "owner": "riverhog_client.processing.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
