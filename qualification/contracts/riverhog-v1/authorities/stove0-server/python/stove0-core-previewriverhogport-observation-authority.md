# stove0_core.PreviewRiverhogPort.observation_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-previewriverhogport-observati-34d88382fa:cce3bd9314 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d4df22b01d"></a>
| Field | Shape |
|---|---|
| <a id="s-7203bcb66c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-fd4512aaa4"></a>`distribution` | "stove0-server" |
| <a id="s-dcec504006"></a>`module` | "stove0_core" |
| <a id="s-92fc8bfd5a"></a>`name` | "observation_authority" |
| <a id="s-e106f0850c"></a>`owner` | "stove0_core.PreviewRiverhogPort" |
| <a id="s-1b55c94a87"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.PreviewRiverhogPort](stove0-core-previewriverhogport.md)

## Governing policies

- <a id="pa-b8fe7b4cd9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.PreviewRiverhogPort.observation_authority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c48fdc398b6763b464b2c36f1734fce44e3247af071dd4dcaf73d4776fcc400d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_authority",
  "owner": "stove0_core.PreviewRiverhogPort",
  "unit": "member"
}
```
