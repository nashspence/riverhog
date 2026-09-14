# stove0_core.InMemoryWorkStore.target_source_edge_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-target-sour-e4eb42503d:6f63781d37 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d5a4b2f0c"></a>
- <a id="s-68c4f6eee6"></a>`distribution`: `stove0-server`
- <a id="s-6384ec7abb"></a>`module`: `stove0_core`
- <a id="s-e6bc46375b"></a>`name`: `target_source_edge_page`
- <a id="s-cb989d5910"></a>`owner`: `stove0_core.InMemoryWorkStore`
- <a id="s-f3dc30538b"></a>`unit`: `member`

### Declared structure

- <a id="s-084e4021ca"></a>`kind`: `"method"`
- <a id="s-dfb4a24909"></a>`signature`: `"'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str \| None\\', after_input_id: \\'str \| None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"`

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-2406eb3120"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.target_source_edge_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c917b98ba696c9a3b032a6f57fce03594b9f2f875d53f7aef1155448a718d16d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str | None\\', after_input_id: \\'str | None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_source_edge_page",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
