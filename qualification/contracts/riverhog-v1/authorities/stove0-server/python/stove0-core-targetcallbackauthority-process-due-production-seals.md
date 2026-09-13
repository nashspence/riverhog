# stove0_core.TargetCallbackAuthority.process_due_production_seals

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetcallbackauthority-proce-a48dcb3894:0b5bd52ed5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-572679f538"></a>
| Field | Shape |
|---|---|
| <a id="s-4252e16b69"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b5a62d54b4"></a>`distribution` | "stove0-server" |
| <a id="s-c95fec8bd7"></a>`module` | "stove0_core" |
| <a id="s-c43a37bfe2"></a>`name` | "process_due_production_seals" |
| <a id="s-2e87b542b0"></a>`owner` | "stove0_core.TargetCallbackAuthority" |
| <a id="s-c3d178d025"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.TargetCallbackAuthority](stove0-core-targetcallbackauthority.md)

## Governing policies

- <a id="pa-89d6566864"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TargetCallbackAuthority.process_due_production_seals`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6beee3bdc0fe12aebf473fe53446e23a04c65902f6f257349822f3438ddc41dd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, limit: 'int' = 1) -> 'int'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "process_due_production_seals",
  "owner": "stove0_core.TargetCallbackAuthority",
  "unit": "member"
}
```
