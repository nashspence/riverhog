# stove0_core.SqlAlchemyStateStore.target_source_edge_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-target-s-4ea38efeed:4c30c8f24c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6dcf999509"></a>
- <a id="s-8bc60b8142"></a>`distribution`: `stove0-server`
- <a id="s-7fffab3899"></a>`module`: `stove0_core`
- <a id="s-3529f0217e"></a>`name`: `target_source_edge_page`
- <a id="s-5784ddbaad"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-bca8f42859"></a>`unit`: `member`

### Declared structure

- <a id="s-23b0e331ad"></a>`kind`: `"method"`
- <a id="s-bd87307f18"></a>`signature`: `"'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str \| None\\', after_input_id: \\'str \| None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-a2036ec585"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.target_source_edge_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca177bf4335001fdf9165176b1b7cc669db7597ca1c3b8dd581fc57d66f98fc5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "'(self, work_id: \\'str\\', job_id: \\'str\\', *, order: \"Literal[\\'output\\', \\'input\\']\", after_output_id: \\'str | None\\', after_input_id: \\'str | None\\', limit: \\'int\\') -> \\'tuple[OutputSourceEdge, ...]\\''"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_source_edge_page",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
