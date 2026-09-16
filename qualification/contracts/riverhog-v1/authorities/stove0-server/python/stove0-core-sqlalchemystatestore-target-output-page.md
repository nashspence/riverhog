# stove0_core.SqlAlchemyStateStore.target_output_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-target-output-page:9ba4796356 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6e5aeb8bcd"></a>
- <a id="s-b9d2f3e99d"></a>`distribution`: `stove0-server`
- <a id="s-1ed9debc6d"></a>`module`: `stove0_core`
- <a id="s-369ded6569"></a>`name`: `target_output_page`
- <a id="s-27c91a89d4"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-32f0c2195d"></a>`unit`: `member`

### Declared structure

- <a id="s-ed320dd980"></a>`kind`: `"method"`
- <a id="s-3741d10b89"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str \| None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-a733699b81"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.target_output_page`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 844b44f0ca657133095d7a21fc0c90a046285d3f50f022349a38fd303f0cca69 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_id: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_output_page",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
