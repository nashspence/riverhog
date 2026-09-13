# riverhog_storage_adapter_protocol.ReadReadiness

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-readreadiness:56a33884d7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2bc4b23484"></a>
| Field | Shape |
|---|---|
| <a id="s-e14c3e2ef9"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-800c0d1905"></a>`distribution` | "riverhog-storage-adapter-protocol" |
| <a id="s-b7527c5d7b"></a>`module` | "riverhog_storage_adapter_protocol" |
| <a id="s-6cc2f36cf4"></a>`name` | "ReadReadiness" |
| <a id="s-965b52ebe3"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7944bd3e6a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ReadReadiness`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3f6dd1529369f4eb1e9e4e8c000c0a79429bd34124bd3a90e53cf89ac43495c -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "ReadReadiness",
  "unit": "export"
}
```
