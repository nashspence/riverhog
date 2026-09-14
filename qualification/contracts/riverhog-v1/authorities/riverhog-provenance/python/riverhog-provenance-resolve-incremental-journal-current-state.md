# riverhog_provenance.resolve_incremental_journal_current_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-resolve-incremental-j-51967c1133:30d29f544e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-71327dba06"></a>
- <a id="s-27f3cf2910"></a>`distribution`: `riverhog-provenance`
- <a id="s-789132f020"></a>`module`: `riverhog_provenance`
- <a id="s-cca27691ba"></a>`name`: `resolve_incremental_journal_current_state`
- <a id="s-6bca8cb9d2"></a>`unit`: `export`

### Declared structure

- <a id="s-8cece760a4"></a>`kind`: `"function"`
- <a id="s-2cf5bdfa01"></a>`signature`: `"\"(*, primary_lineage_id: 'str', binding_json: 'str', state_json: 'str') -> 'tuple[str, str, int, str]'\""`

## Governing policies

- <a id="pa-dd48f517af"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.resolve_incremental_journal_current_state`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d82b8c17ea69c6bb38cf4f317330b66e8c0478ba27430f289073c008a671331 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, primary_lineage_id: 'str', binding_json: 'str', state_json: 'str') -> 'tuple[str, str, int, str]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "resolve_incremental_journal_current_state",
  "unit": "export"
}
```
