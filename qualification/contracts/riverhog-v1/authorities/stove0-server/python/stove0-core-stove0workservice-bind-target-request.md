# stove0_core.Stove0WorkService.bind_target_request

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-bind-target-request:d767357ffe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-54d1a46b47"></a>
- <a id="s-9cf2b37c02"></a>`distribution`: `stove0-server`
- <a id="s-79b694197c"></a>`module`: `stove0_core`
- <a id="s-522cb99f53"></a>`name`: `bind_target_request`
- <a id="s-2f1e1a88fc"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-d9a1a3ec85"></a>`unit`: `member`

### Declared structure

- <a id="s-25fb566f67"></a>`kind`: `"method"`
- <a id="s-433586677a"></a>`signature`: `"\"(self, work_id: 'str', request: 'TargetJobRequest', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-57e3a82f39"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.bind_target_request`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d26a750bca9613d8d6707693d773615e31c82e8118d9b75237349e6c22b0525 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', request: 'TargetJobRequest', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "bind_target_request",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
