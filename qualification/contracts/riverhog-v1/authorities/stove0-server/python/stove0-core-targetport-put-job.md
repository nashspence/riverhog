# stove0_core.TargetPort.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport-put-job:735cda8e5b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8cc1acb9b9"></a>
| Field | Shape |
|---|---|
| <a id="s-b90d289c92"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f137faaa0c"></a>`distribution` | "stove0-server" |
| <a id="s-5df2653f93"></a>`module` | "stove0_core" |
| <a id="s-eb046697b1"></a>`name` | "put_job" |
| <a id="s-e1b3554289"></a>`owner` | "stove0_core.TargetPort" |
| <a id="s-c4dc99b8d1"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.TargetPort](stove0-core-targetport.md)

## Governing policies

- <a id="pa-9bcb03d674"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TargetPort.put_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b05699cbb40397b0dd3688fc3ff9e6d147b8ec760a2a059f71b3845497adc83e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "put_job",
  "owner": "stove0_core.TargetPort",
  "unit": "member"
}
```
