# stove0_core.PreviewRiverhogPort.abandon_preview_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewriverhogport-abandon-p-841ad061e5:7d82371279 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-21bc857cf0"></a>
| Field | Shape |
|---|---|
| <a id="s-48e7fe88a3"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-de37df3fd7"></a>`distribution` | "stove0-server" |
| <a id="s-de4b5cc0a7"></a>`module` | "stove0_core" |
| <a id="s-8146afe432"></a>`name` | "abandon_preview_claim" |
| <a id="s-9a519a1604"></a>`owner` | "stove0_core.PreviewRiverhogPort" |
| <a id="s-58c80d8847"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.PreviewRiverhogPort](stove0-core-previewriverhogport.md)

## Governing policies

- <a id="pa-717549a087"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PreviewRiverhogPort.abandon_preview_claim`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea2bf6084ba9e38cbf6722c004b1c79abd6dd6dcad3d57451132d57d6a433629 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WorkflowPreviewRequest', claim: 'ClaimBinding') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "abandon_preview_claim",
  "owner": "stove0_core.PreviewRiverhogPort",
  "unit": "member"
}
```
