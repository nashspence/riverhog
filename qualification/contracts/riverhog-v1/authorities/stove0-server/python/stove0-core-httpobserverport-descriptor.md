# stove0_core.HttpObserverPort.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httpobserverport-descriptor:607496a491 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a0a9e50cf"></a>
| Field | Shape |
|---|---|
| <a id="s-c4bb950094"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-68339f68b1"></a>`distribution` | "stove0-server" |
| <a id="s-87f984e010"></a>`module` | "stove0_core" |
| <a id="s-7064105d00"></a>`name` | "descriptor" |
| <a id="s-19e486bfff"></a>`owner` | "stove0_core.HttpObserverPort" |
| <a id="s-8dfdb938b3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.HttpObserverPort](stove0-core-httpobserverport.md)

## Governing policies

- <a id="pa-cee9efb91e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.HttpObserverPort.descriptor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ed33ed1d49be71cb1748f4cdf76c8379a75a84ad479d0b0d653fa09fb3175095 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, registration_id: 'str') -> 'ObserverDescriptor'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "descriptor",
  "owner": "stove0_core.HttpObserverPort",
  "unit": "member"
}
```
