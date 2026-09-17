# stove0_core.SqlAlchemyStateStore.target_disposition_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-target-d-7a89be29b2:8bc6a449f7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5f140051b0"></a>
- <a id="s-02d4b0c1f8"></a>`distribution`: `stove0-server`
- <a id="s-a50708a844"></a>`module`: `stove0_core`
- <a id="s-8d443809c0"></a>`name`: `target_disposition_page`
- <a id="s-e3f2909fcd"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-364c709fcb"></a>`unit`: `member`

### Declared structure

- <a id="s-ef6b7e75fc"></a>`kind`: `"method"`
- <a id="s-a84d3cab7e"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str \| None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-5f3e23d0a6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.target_disposition_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f28b6a5cb4d02aff675422b3b5dd45eb53524854cd4b17379f039a9da82daca9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[InputDispositionDeclaration, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_disposition_page",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
