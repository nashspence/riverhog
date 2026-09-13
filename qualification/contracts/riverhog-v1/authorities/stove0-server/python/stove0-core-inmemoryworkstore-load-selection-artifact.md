# stove0_core.InMemoryWorkStore.load_selection_artifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-load-select-be10540ec8:cab853e8ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-005ee4f7f9"></a>
| Field | Shape |
|---|---|
| <a id="s-6d6e428001"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d61ca69f0c"></a>`distribution` | "stove0-server" |
| <a id="s-c3fe02e9ad"></a>`module` | "stove0_core" |
| <a id="s-572bd3b27e"></a>`name` | "load_selection_artifact" |
| <a id="s-f828d9b058"></a>`owner` | "stove0_core.InMemoryWorkStore" |
| <a id="s-d1b930025f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-c1b88f3de4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.load_selection_artifact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5344195944533185c5e7c8dd69590c4f21a82c717079a4df919bc9b97c7d2606 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', artifact_id: 'str') -> 'ArtifactSubject | None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_selection_artifact",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
