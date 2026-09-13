# riverhog_client.transform.TransformWorkspace.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-transformworkspace-enter:2b691ebc81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8bc6df6269"></a>
| Field | Shape |
|---|---|
| <a id="s-f2d26eda93"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ccd6a9218e"></a>`distribution` | "riverhog-client" |
| <a id="s-ef811e9c26"></a>`module` | "riverhog_client.transform" |
| <a id="s-abe310df0e"></a>`name` | "__enter__" |
| <a id="s-a0e474302b"></a>`owner` | "riverhog_client.transform.TransformWorkspace" |
| <a id="s-edfdce4be4"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.TransformWorkspace](riverhog-client-transform-transformworkspace.md)

## Governing policies

- <a id="pa-26eca8808a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.TransformWorkspace.__enter__`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c7b213b750bdfa36b590288a82fc232f838af89df5e0c258ee77fa0994dd2670 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "__enter__",
  "owner": "riverhog_client.transform.TransformWorkspace",
  "unit": "member"
}
```
