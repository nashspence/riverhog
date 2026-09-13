# riverhog_protocol.CatalogSyncRevision

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-catalogsyncrevision:8dd3d6c4db -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-60a160ae9d"></a>
| Field | Shape |
|---|---|
| <a id="s-049f3a63bb"></a>`contract` | type="typing._AnnotatedAlias"; additional keys=`kind` |
| <a id="s-45e4a3e160"></a>`distribution` | "riverhog-protocol" |
| <a id="s-ef09db685e"></a>`module` | "riverhog_protocol" |
| <a id="s-a52029df91"></a>`name` | "CatalogSyncRevision" |
| <a id="s-1773b65d89"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1e8864b869"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CatalogSyncRevision`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 188957c5e4fee324fad49817229f7b1abb82a0fdbac14abc69c260c356553799 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CatalogSyncRevision",
  "unit": "export"
}
```
