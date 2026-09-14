# stove0_core.Stove0Coordinator.cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0coordinator-cancel:f644e3c0e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc3bba1fe6"></a>
- <a id="s-1b763dc0c1"></a>`distribution`: `stove0-server`
- <a id="s-ad672a755b"></a>`module`: `stove0_core`
- <a id="s-423f758ed3"></a>`name`: `cancel`
- <a id="s-9baae98c1a"></a>`owner`: `stove0_core.Stove0Coordinator`
- <a id="s-c9a9bceb50"></a>`unit`: `member`

### Declared structure

- <a id="s-758f979e2c"></a>`kind`: `"method"`
- <a id="s-e8f0641763"></a>`signature`: `"\"(self, work_id: 'str') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.Stove0Coordinator](stove0-core-stove0coordinator.md)

## Governing policies

- <a id="pa-39cfdff256"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0Coordinator.cancel`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7b0823afc64878bd7ea014d017794fbd382a728e0600d347e1928bcba8b86ac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "cancel",
  "owner": "stove0_core.Stove0Coordinator",
  "unit": "member"
}
```
