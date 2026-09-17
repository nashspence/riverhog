# stove0_core.Stove0WorkService.record_retired

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0workservice-record-retired:5e5d36d8ed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e3d83194a5"></a>
- <a id="s-da7bf7e17b"></a>`distribution`: `stove0-server`
- <a id="s-975367a56f"></a>`module`: `stove0_core`
- <a id="s-98d8231c59"></a>`name`: `record_retired`
- <a id="s-9d25f9d6fc"></a>`owner`: `stove0_core.Stove0WorkService`
- <a id="s-708b87b94b"></a>`unit`: `member`

### Declared structure

- <a id="s-697c99b35a"></a>`kind`: `"method"`
- <a id="s-5bf4d3a50e"></a>`signature`: `"\"(self, work_id: 'str', collection_id: 'int', *, expected_revision: 'int') -> 'WorkRecord'\""`

## Maintained corroboration

### Related interface records

- [Stove0WorkService](stove0-core-stove0workservice.md)

## Governing policies

- <a id="pa-c207a38465"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0WorkService.record_retired`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f1073eb678869d64918fc9a890b51fb65513eca1b9492f8ff028e8519323f377 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', collection_id: 'int', *, expected_revision: 'int') -> 'WorkRecord'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_retired",
  "owner": "stove0_core.Stove0WorkService",
  "unit": "member"
}
```

</details>
