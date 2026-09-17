# riverhog_client.transform.WorkspaceAssurance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-workspaceassurance:e4166a4342 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de0a986123"></a>
- <a id="s-5e2fb41b38"></a>`distribution`: `riverhog-client`
- <a id="s-438fcb33f7"></a>`module`: `riverhog_client.transform`
- <a id="s-f7c6bfef88"></a>`name`: `WorkspaceAssurance`
- <a id="s-4755062df7"></a>`unit`: `export`

### Declared structure

- <a id="s-e5cbf54a1d"></a>`kind`: `"object"`
- <a id="s-1a2c0ef0f0"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-b32863cc58"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.WorkspaceAssurance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4083154a5c41efb6a5081ea88d3f4206137b1278e09c7fe7666fa9b19b4b1103 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "WorkspaceAssurance",
  "unit": "export"
}
```

</details>
