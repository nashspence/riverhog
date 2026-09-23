# stove0_core.SqlAlchemyStateStore.iter_target_outputs_by_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-tar-459f2a5134:8c9ee57d3a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9bfd0b632e"></a>
- <a id="s-9b0bf7b75c"></a>`distribution`: `stove0-server`
- <a id="s-5ab4e61846"></a>`module`: `stove0_core`
- <a id="s-7d3bd58c1c"></a>`name`: `iter_target_outputs_by_path`
- <a id="s-ea9de11da4"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-f5c0438254"></a>`unit`: `member`

### Declared structure

- <a id="s-240c33214e"></a>`kind`: `"method"`
- <a id="s-8b1936ae1b"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-7d0cdaecd7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_target_outputs_by_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 92d68f30aecfa0b73b76e74fd339c55f192b0370fce635e44e2a8ec1566e355e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[OutputArtifact]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_outputs_by_path",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
