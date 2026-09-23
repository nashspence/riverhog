# stove0_core.Stove0WorkService.record_target_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-record-target-status:e662d684d1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e29a11199e"></a>
- <a id="s-c5f2272f2b"></a>`distribution`: `stove0-server`
- <a id="s-9014414f37"></a>`module`: `stove0_core`
- <a id="s-8ddd509c29"></a>`name`: `record_target_status`
- <a id="s-fac7afe155"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-53e9cd4f44"></a>`unit`: `member`

### Declared structure

- <a id="s-9ee3321603"></a>`kind`: `"method"`
- <a id="s-ac35fc0c53"></a>`signature`: `"\"(self, work_id: 'str', status: 'TargetJobStatus', *, operation: 'OperationContract', expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-4db26bc967"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.record_target_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8c6f223494bfdc194326a70f23671025821acbd7f8d905fced62edbcd33cca3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', status: 'TargetJobStatus', *, operation: 'OperationContract', expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_status",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
