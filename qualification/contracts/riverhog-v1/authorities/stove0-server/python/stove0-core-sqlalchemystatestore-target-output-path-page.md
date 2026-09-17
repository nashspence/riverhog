# stove0_core.SqlAlchemyStateStore.target_output_path_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-target-o-d9b6d8a8d2:64def2ddaf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6433d7050d"></a>
- <a id="s-f04f0e59b8"></a>`distribution`: `stove0-server`
- <a id="s-dd974f9133"></a>`module`: `stove0_core`
- <a id="s-8133e949f2"></a>`name`: `target_output_path_page`
- <a id="s-ba2edd222f"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-d56901f5e3"></a>`unit`: `member`

### Declared structure

- <a id="s-98fcc1ce5f"></a>`kind`: `"method"`
- <a id="s-524ecf183a"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str \| None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-50b34f12c9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.target_output_path_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9355904aa4065792e05e9635746fd4eb643e140ab3abc021d4cd0360835cd5d3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_output_path_page",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
