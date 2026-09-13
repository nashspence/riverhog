# stove0_core.ClassificationAdmissionService.get_admission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-classificationadmissionservic-063af7ea93:164c8a5375 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c4e5749054"></a>
| Field | Shape |
|---|---|
| <a id="s-5fe44fc00a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1f45d3d6f6"></a>`distribution` | "stove0-server" |
| <a id="s-397a08bd2b"></a>`module` | "stove0_core" |
| <a id="s-6f2a482014"></a>`name` | "get_admission" |
| <a id="s-245850e126"></a>`owner` | "stove0_core.ClassificationAdmissionService" |
| <a id="s-4c50ecdc11"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.ClassificationAdmissionService](stove0-core-classificationadmissionservice.md)

## Governing policies

- <a id="pa-bed39a8a33"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ClassificationAdmissionService.get_admission`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56f76afd3223dc37fd7ab65e26accde3136a6bff3cdf90709a40d6c26a9c24cd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, admission_id: 'str') -> 'AdmissionView'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_admission",
  "owner": "stove0_core.ClassificationAdmissionService",
  "unit": "member"
}
```
