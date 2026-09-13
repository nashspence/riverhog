# stove0_core.InMemoryWorkStore.selection_artifact_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-selection-a-b158cddca8:a58ea425c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de96a488ed"></a>
| Field | Shape |
|---|---|
| <a id="s-62fcd20ec3"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-d42dbcb625"></a>`distribution` | "stove0-server" |
| <a id="s-bccdc6893d"></a>`module` | "stove0_core" |
| <a id="s-d177c1bce1"></a>`name` | "selection_artifact_page" |
| <a id="s-45038748cf"></a>`owner` | "stove0_core.InMemoryWorkStore" |
| <a id="s-60dd2bf822"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-dd3560a671"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.selection_artifact_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86292f0c5ed775583589547de9ff8d20fc83a91d0b96b19a6f3eed08a276d9fe -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', *, continuation: 'str | None', limit: 'int') -> 'tuple[tuple[ArtifactSubject, ...], str | None, bool]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "selection_artifact_page",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
