# riverhog_provenance.validate_journal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-validate-journal:a80e6ea3ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc6187799f"></a>
- <a id="s-918aeec642"></a>`distribution`: `riverhog-provenance`
- <a id="s-134e80fc66"></a>`module`: `riverhog_provenance`
- <a id="s-ed9ef1eb8b"></a>`name`: `validate_journal`
- <a id="s-f4c0df1f2c"></a>`unit`: `export`

### Declared structure

- <a id="s-9fb8ae7bef"></a>`kind`: `"function"`
- <a id="s-be3eb60ac8"></a>`signature`: `"\"(content: 'bytes') -> 'JournalSummary'\""`

## Governing policies

- <a id="pa-0ef1672208"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.validate_journal`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 46120053d6d025b35b0d84cc44996ba2278e00a0e7198db206f1b9a34eec4c5f -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(content: 'bytes') -> 'JournalSummary'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "validate_journal",
  "unit": "export"
}
```
