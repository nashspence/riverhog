# stove0_core.RecipePlanner.target_input_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-target-input-selection:d53525130e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e4325ab3d7"></a>
| Field | Shape |
|---|---|
| <a id="s-a296a1199b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-c59a94fcee"></a>`distribution` | "stove0-server" |
| <a id="s-aebf211dae"></a>`module` | "stove0_core" |
| <a id="s-31d9ccd04b"></a>`name` | "target_input_selection" |
| <a id="s-ff2768f17c"></a>`owner` | "stove0_core.RecipePlanner" |
| <a id="s-be470a2b53"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-c43cc28abd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.target_input_selection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9ee5dec57021559ee424fb9f85379e95dc5f959c77cf776d5ad7fde16be3144 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, plan: 'WorkflowPlan', selections: 'Mapping[str, ArtifactSelection]') -> 'ArtifactSelection'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_input_selection",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```
