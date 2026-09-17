# stove0_core.Stove0WorkService.record_coordination_settlement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-record-coor-5125e7e572:e9f0f5571f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-158fdcf4bc"></a>
- <a id="s-337ae1b582"></a>`distribution`: `stove0-server`
- <a id="s-96e60ae2de"></a>`module`: `stove0_core`
- <a id="s-5310052c7c"></a>`name`: `record_coordination_settlement`
- <a id="s-a946aa2454"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-ef1f32654d"></a>`unit`: `member`

### Declared structure

- <a id="s-249447465e"></a>`kind`: `"method"`
- <a id="s-85baa05398"></a>`signature`: `"\"(self, work_id: 'str', settlement: 'CoordinationSettlement', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-40f40d3841"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.record_coordination_settlement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35c5a31b9106a7a5b85d7e6eedceb1e9d6b097cac72452a2dcf4aa680293992d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', settlement: 'CoordinationSettlement', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_coordination_settlement",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
