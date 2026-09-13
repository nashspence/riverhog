# riverhog_client.transform.CancellationCheck

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-cancellationcheck:a74b869247 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39482423e9"></a>
| Field | Shape |
|---|---|
| <a id="s-7ac35866c8"></a>`contract` | type="collections.abc._CallableGenericAlias"; additional keys=`kind` |
| <a id="s-f7a0e24e8b"></a>`distribution` | "riverhog-client" |
| <a id="s-9f252757fa"></a>`module` | "riverhog_client.transform" |
| <a id="s-8a841d56b8"></a>`name` | "CancellationCheck" |
| <a id="s-63f2c4e03b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9a9c68c27a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.CancellationCheck`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0eb10b972a5d17a09e657d6375ea23ff351b454d14c76b07790eb04e0f5bd3a1 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "collections.abc._CallableGenericAlias"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "CancellationCheck",
  "unit": "export"
}
```
