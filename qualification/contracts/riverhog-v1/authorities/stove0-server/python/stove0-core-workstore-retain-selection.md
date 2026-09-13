# stove0_core.WorkStore.retain_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-retain-selection:987fb44d20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2190de823"></a>
| Field | Shape |
|---|---|
| <a id="s-ba53436406"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-ec60969e1d"></a>`distribution` | "stove0-server" |
| <a id="s-36688e4855"></a>`module` | "stove0_core" |
| <a id="s-4f91dc1144"></a>`name` | "retain_selection" |
| <a id="s-89e9f6b7a4"></a>`owner` | "stove0_core.WorkStore" |
| <a id="s-5e7ae2ce7c"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-ef6d847ba1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.retain_selection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6a51de6dd27175d9b8199de7fbca3d1903bf94951c3fee7adf9141b968a0cf3b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection: 'ArtifactSelection') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retain_selection",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
