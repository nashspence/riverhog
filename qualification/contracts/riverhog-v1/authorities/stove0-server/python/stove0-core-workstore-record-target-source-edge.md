# stove0_core.WorkStore.record_target_source_edge

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-record-target-source-edge:1a116faa5b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3ce991c34f"></a>
- <a id="s-c9a086811b"></a>`distribution`: `stove0-server`
- <a id="s-3185bc23d8"></a>`module`: `stove0_core`
- <a id="s-ad87ff762e"></a>`name`: `record_target_source_edge`
- <a id="s-7d967ccbf2"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-0d59711fe4"></a>`unit`: `member`

### Declared structure

- <a id="s-d874899bfe"></a>`kind`: `"method"`
- <a id="s-d7dbef6a29"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-a8082687e7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.record_target_source_edge`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89987ef25a9b7a5e49dab93ab28ada53b928a8279524b4b235275a81cd19dcc0 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', edge: 'OutputSourceEdge') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_target_source_edge",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```

</details>
