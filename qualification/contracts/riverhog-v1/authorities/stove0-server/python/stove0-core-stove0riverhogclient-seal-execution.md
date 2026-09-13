# stove0_core.Stove0RiverhogClient.seal_execution

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-seal-execution:18ecc5ee55 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cdb562516d"></a>
| Field | Shape |
|---|---|
| <a id="s-ef2eab2700"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-14b3d7db4a"></a>`distribution` | "stove0-server" |
| <a id="s-650682e768"></a>`module` | "stove0_core" |
| <a id="s-d9e3a9e3d9"></a>`name` | "seal_execution" |
| <a id="s-d84550ba37"></a>`owner` | "stove0_core.Stove0RiverhogClient" |
| <a id="s-c4fa01b818"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-c49ed131cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.seal_execution`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12ca2a70901e8d247b282179c0822aa63d2181d733bf6a8dac7ce866f42f61ec -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', plan: 'WorkflowPlan', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_execution",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
