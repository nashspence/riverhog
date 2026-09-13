# stove0_core.Stove0RiverhogClient.project_target_source_edges

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-project-018df198c4:0325fddd26 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b786082bb0"></a>
| Field | Shape |
|---|---|
| <a id="s-63fbb43d82"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-15bc279384"></a>`distribution` | "stove0-server" |
| <a id="s-320b987da5"></a>`module` | "stove0_core" |
| <a id="s-8de52df5ee"></a>`name` | "project_target_source_edges" |
| <a id="s-3e9cbb1e7d"></a>`owner` | "stove0_core.Stove0RiverhogClient" |
| <a id="s-3c238d41c2"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-e0be4224c7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.project_target_source_edges`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07628b1ff11982bba66460e519269e973ca9821d85ce128f955845ad270abe6c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', edges: 'Sequence[ArtifactDispositionOutput]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "project_target_source_edges",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
