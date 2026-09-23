# stove0_core.SqlAlchemyStateStore.iter_target_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-target-outputs:ccff6909de -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-de2ef7f6cf"></a>
- <a id="s-e36ef5dcd7"></a>`distribution`: `stove0-server`
- <a id="s-50582f052a"></a>`module`: `stove0_core`
- <a id="s-42fc4bf8dc"></a>`name`: `iter_target_outputs`
- <a id="s-1d19ca7fb3"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-101842ce64"></a>`unit`: `member`

### Declared structure

- <a id="s-ab5e2777d7"></a>`kind`: `"method"`
- <a id="s-d15c9e5bdb"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-8403f3740c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_target_outputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0f4d1b6c87ce52eafaf3be49ae61798f263d8cf33dca6d94c5b1af6db50ba157 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_outputs",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
