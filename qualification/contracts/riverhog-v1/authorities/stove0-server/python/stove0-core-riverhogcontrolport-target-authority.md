# stove0_core.RiverhogControlPort.target_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-target-authority:821297da74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7f07360cfd"></a>
| Field | Shape |
|---|---|
| <a id="s-c13413a049"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9086c5f43e"></a>`distribution` | "stove0-server" |
| <a id="s-56cca38533"></a>`module` | "stove0_core" |
| <a id="s-b97c2b42ee"></a>`name` | "target_authority" |
| <a id="s-c65196eede"></a>`owner` | "stove0_core.RiverhogControlPort" |
| <a id="s-c54b39cd1d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-adef30451f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.target_authority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f84ad0f293d1ca74af1d4183c9f171dadb597eba9c9184593144ea8938411eab -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', evidence: 'ControllerEvidence', target_plan: 'TargetPlan', inputs: 'Iterable[ArtifactSubject]') -> 'TargetInvocationAuthority'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_authority",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```
