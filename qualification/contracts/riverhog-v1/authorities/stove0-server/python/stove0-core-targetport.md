# stove0_core.TargetPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-targetport:ef9508ec89 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b9ac746a3c"></a>
| Field | Shape |
|---|---|
| <a id="s-8585f97b28"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-75f089bdb4"></a>`distribution` | "stove0-server" |
| <a id="s-4d8a896dc0"></a>`module` | "stove0_core" |
| <a id="s-e18209cee7"></a>`name` | "TargetPort" |
| <a id="s-10f16b65b2"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.TargetPort.cancel_job](stove0-core-targetport-cancel-job.md)
- [stove0_core.TargetPort.contract](stove0-core-targetport-contract.md)
- [stove0_core.TargetPort.get_job](stove0-core-targetport-get-job.md)
- [stove0_core.TargetPort.preflight](stove0-core-targetport-preflight.md)
- [stove0_core.TargetPort.put_job](stove0-core-targetport-put-job.md)

## Governing policies

- <a id="pa-8ef51f75df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.TargetPort`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 672f8f7ce3e2339c3d82ef624acd594479f8687e2458715f465a293384160ab3 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "TargetPort",
  "unit": "export"
}
```
