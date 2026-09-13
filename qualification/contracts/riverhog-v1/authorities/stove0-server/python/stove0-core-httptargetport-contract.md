# stove0_core.HttpTargetPort.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httptargetport-contract:24b43626be -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d93f1141a4"></a>
| Field | Shape |
|---|---|
| <a id="s-90ea416f4a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-128f98e0f0"></a>`distribution` | "stove0-server" |
| <a id="s-675c7415af"></a>`module` | "stove0_core" |
| <a id="s-8c7c61a168"></a>`name` | "contract" |
| <a id="s-dd5e8339e8"></a>`owner` | "stove0_core.HttpTargetPort" |
| <a id="s-114fa295db"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.HttpTargetPort](stove0-core-httptargetport.md)

## Governing policies

- <a id="pa-f357ad23e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.HttpTargetPort.contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e0420fee0b9256303894ddb9468914251344032e2bc5fde005f9066c5d4109c2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'TargetContract'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "contract",
  "owner": "stove0_core.HttpTargetPort",
  "unit": "member"
}
```
