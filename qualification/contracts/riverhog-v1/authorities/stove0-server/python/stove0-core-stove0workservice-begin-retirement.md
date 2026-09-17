# stove0_core.Stove0WorkService.begin_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-begin-retirement:b906dcee10 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9ed9cae0b"></a>
- <a id="s-b226d3d240"></a>`distribution`: `stove0-server`
- <a id="s-89e06a53da"></a>`module`: `stove0_core`
- <a id="s-8cf645fb09"></a>`name`: `begin_retirement`
- <a id="s-f2a957a0c3"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-20577863e9"></a>`unit`: `member`

### Declared structure

- <a id="s-e0e08aa844"></a>`kind`: `"method"`
- <a id="s-a83dce578f"></a>`signature`: `"\"(self, work_id: 'str', collection_ids: 'Sequence[int]', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-365ee6979d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.begin_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 81f0749a03e6ae3a619e55d56036cdd2983ec85e74eadd2ba6e10e467a618739 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', collection_ids: 'Sequence[int]', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_retirement",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
