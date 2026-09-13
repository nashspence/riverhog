# stove0_protocol.BranchPlan.bind_child_work

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchplan-bind-child-work:2e3debab19 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dfc9db5e6e"></a>
| Field | Shape |
|---|---|
| <a id="s-0c9b4efae3"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b02f3b4146"></a>`distribution` | "stove0-protocol" |
| <a id="s-63f4c714c2"></a>`module` | "stove0_protocol" |
| <a id="s-8a2223eeb3"></a>`name` | "bind_child_work" |
| <a id="s-482129dee6"></a>`owner` | "stove0_protocol.BranchPlan" |
| <a id="s-4cbf3f7ec3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.BranchPlan](stove0-protocol-branchplan.md)

## Governing policies

- <a id="pa-62cfd3a529"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.BranchPlan.bind_child_work`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3be89c4df438628398edd2a228d9d6f8005ddd6c2babfdd581021611939fb616 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "bind_child_work",
  "owner": "stove0_protocol.BranchPlan",
  "unit": "member"
}
```
