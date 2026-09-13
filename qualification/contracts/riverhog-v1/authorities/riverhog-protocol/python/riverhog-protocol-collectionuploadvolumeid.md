# riverhog_protocol.CollectionUploadVolumeId

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadvolumeid:e8df24fa2f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-762c148a0e"></a>
| Field | Shape |
|---|---|
| <a id="s-00ce4fa602"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-2f3a381ab1"></a>`distribution` | "riverhog-protocol" |
| <a id="s-e1c2867b87"></a>`module` | "riverhog_protocol" |
| <a id="s-3027499464"></a>`name` | "CollectionUploadVolumeId" |
| <a id="s-658f60b8b5"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a0ee8ca886"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadVolumeId`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd02a55e84ced17b55e4d3dcab42d11b4a1d86396d4eb4296769b0f26ff8a3be -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionUploadVolumeId",
  "unit": "export"
}
```
