# stove0_core.PlanningPort.observation_requests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-planningport-observation-requests:6ebb1f1e0a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5415c29d52"></a>
- <a id="s-2e9ace16c3"></a>`distribution`: `stove0-server`
- <a id="s-a3e7bdba95"></a>`module`: `stove0_core`
- <a id="s-76a814168a"></a>`name`: `observation_requests`
- <a id="s-0d0267c038"></a>`owner`: `stove0_core.PlanningPort`
- <a id="s-d4db9cd6ae"></a>`unit`: `member`

### Declared structure

- <a id="s-ed87644ab6"></a>`kind`: `"method"`
- <a id="s-2cb94e0bd3"></a>`signature`: `"\"(self, work: 'WorkIdentity') -> 'tuple[ObservationRequest, ...]'\""`

## Maintained corroboration

### Related interface records

- [PlanningPort](stove0-core-planningport.md)

## Governing policies

- <a id="pa-62c0284488"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.PlanningPort.observation_requests`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3e8f04aa68e4830dc98e2de6f0c43f8ede07d618342b2c12dae1bdb23c0e583 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity') -> 'tuple[ObservationRequest, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_requests",
  "owner": "stove0_core.PlanningPort",
  "unit": "member"
}
```

</details>
