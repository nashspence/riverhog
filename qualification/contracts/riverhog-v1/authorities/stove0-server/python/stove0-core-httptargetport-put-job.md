# stove0_core.HttpTargetPort.put_job

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport-put-job:af0677879b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2cfb06e907"></a>
| Field | Shape |
|---|---|
| <a id="s-fb95860aeb"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-92d81cf704"></a>`distribution` | "stove0-server" |
| <a id="s-61b59c73d2"></a>`module` | "stove0_core" |
| <a id="s-24607d4013"></a>`name` | "put_job" |
| <a id="s-0c204579a8"></a>`owner` | "stove0_core.HttpTargetPort" |
| <a id="s-fbfc757c53"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.HttpTargetPort](stove0-core-httptargetport.md)

## Governing policies

- <a id="pa-16f560a3da"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort.put_job`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f3c76f9608390b66c2bb79295b752d6d1ffe09d98b2f31051a6c5f74f98347b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str', request: 'TargetJobRequest', *, operation: 'OperationContract') -> 'TargetJobStatus'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "put_job",
  "owner": "stove0_core.HttpTargetPort",
  "unit": "member"
}
```
