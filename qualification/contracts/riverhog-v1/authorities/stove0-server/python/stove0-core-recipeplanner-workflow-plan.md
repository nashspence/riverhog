# stove0_core.RecipePlanner.workflow_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-workflow-plan:cec3fe6c43 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc44503b13"></a>
| Field | Shape |
|---|---|
| <a id="s-b555740b68"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-75c43ce0ed"></a>`distribution` | "stove0-server" |
| <a id="s-56c41666d4"></a>`module` | "stove0_core" |
| <a id="s-82be029fdb"></a>`name` | "workflow_plan" |
| <a id="s-9342ab9d48"></a>`owner` | "stove0_core.RecipePlanner" |
| <a id="s-febce2e2e1"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-6b9dd1f7f8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.workflow_plan`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 74913cf2188ea02b5b25205ccca3b417540a57568ccdf40223c498a777b0abf2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity', observations: 'tuple[ObservationEvidence, ...]', *, nested_observer: 'NestedObservation | None' = None) -> 'BranchSetDecision | WorkInapplicable'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "workflow_plan",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```
