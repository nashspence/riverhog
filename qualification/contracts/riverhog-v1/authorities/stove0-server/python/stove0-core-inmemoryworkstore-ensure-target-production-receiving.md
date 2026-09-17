# stove0_core.InMemoryWorkStore.ensure_target_production_receiving

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-ensure-targ-4e6ae911f2:460d834e2a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc2b6fd068"></a>
- <a id="s-0eeef24673"></a>`distribution`: `stove0-server`
- <a id="s-e4bbaadd26"></a>`module`: `stove0_core`
- <a id="s-5f3404086f"></a>`name`: `ensure_target_production_receiving`
- <a id="s-163dc6ebc9"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-5176f12bcc"></a>`unit`: `member`

### Declared structure

- <a id="s-700f6d8ba1"></a>`kind`: `"method"`
- <a id="s-d4d5eb16cd"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""`

## Maintained corroboration

### Related interface records

- [InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-28bdc92e7c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.ensure_target_production_receiving`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 76e92e37cea7e9bf0115a46d204f6c5d14c2eaf2190be80669ad12882ff70e42 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'TargetProductionSealRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ensure_target_production_receiving",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```

</details>
