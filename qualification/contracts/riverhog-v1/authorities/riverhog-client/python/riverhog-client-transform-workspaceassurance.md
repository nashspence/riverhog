# riverhog_client.transform.WorkspaceAssurance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-workspaceassurance:e4166a4342 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de0a986123"></a>
| Field | Shape |
|---|---|
| <a id="s-53d745cf2a"></a>`contract` | type="typing._LiteralGenericAlias"; additional keys=`kind` |
| <a id="s-5e2fb41b38"></a>`distribution` | "riverhog-client" |
| <a id="s-438fcb33f7"></a>`module` | "riverhog_client.transform" |
| <a id="s-f7c6bfef88"></a>`name` | "WorkspaceAssurance" |
| <a id="s-4755062df7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b32863cc58"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.WorkspaceAssurance`

### Exact owned JSON

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
