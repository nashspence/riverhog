# riverhog_provenance.validate_graph_fragment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-validate-graph-fragment:45b4675c09 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-872daa8a7f"></a>
- <a id="s-0f265d1760"></a>`distribution`: `riverhog-provenance`
- <a id="s-346b1f30b3"></a>`module`: `riverhog_provenance`
- <a id="s-4068750ab6"></a>`name`: `validate_graph_fragment`
- <a id="s-c14b3cc7c3"></a>`unit`: `export`

### Declared structure

- <a id="s-dde77bcdb6"></a>`kind`: `"function"`
- <a id="s-30b16c2a0e"></a>`signature`: `"\"(fragment: 'Mapping[str, Any]', *, contract_schemas: 'Mapping[str, Mapping[str, Any]] \| None' = None, require_known_schemas: 'bool' = False) -> 'None'\""`

## Governing policies

- <a id="pa-df2cf71899"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.validate_graph_fragment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e10eab11c7ca1f378b849bd91ede7f12e2e94b0cebded4c672cdf82fa0136ae9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(fragment: 'Mapping[str, Any]', *, contract_schemas: 'Mapping[str, Mapping[str, Any]] | None' = None, require_known_schemas: 'bool' = False) -> 'None'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "validate_graph_fragment",
  "unit": "export"
}
```

</details>
