# stove0_core.SqlAlchemyStateStore.iter_target_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-tar-d55f987ac7:729ade8ac2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4a9ee91326"></a>
- <a id="s-051a2ed2fc"></a>`distribution`: `stove0-server`
- <a id="s-b9466a1a19"></a>`module`: `stove0_core`
- <a id="s-5e65336996"></a>`name`: `iter_target_dispositions`
- <a id="s-8cdc938ac6"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-c50027d2fb"></a>`unit`: `member`

### Declared structure

- <a id="s-730d101c53"></a>`kind`: `"method"`
- <a id="s-89f71f49bd"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-543b43fefc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_target_dispositions`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc369d0b9ea714738dd6b23bcf9e9cc723a594da1aba7dd0096328f1428f6a67 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str') -> 'Iterator[InputDispositionDeclaration]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_target_dispositions",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
