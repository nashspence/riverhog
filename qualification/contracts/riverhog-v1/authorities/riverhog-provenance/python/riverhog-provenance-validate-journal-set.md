# riverhog_provenance.validate_journal_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-validate-journal-set:ecedc9bc4d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-40fa19372d"></a>
- <a id="s-d163c3a3e9"></a>`distribution`: `riverhog-provenance`
- <a id="s-d488c31e0e"></a>`module`: `riverhog_provenance`
- <a id="s-9f7cafcb46"></a>`name`: `validate_journal_set`
- <a id="s-450514d129"></a>`unit`: `export`

### Declared structure

- <a id="s-3279cb9e25"></a>`kind`: `"function"`
- <a id="s-f8e5a1ad52"></a>`signature`: `"\"(journals: 'Mapping[str, bytes]') -> 'dict[str, JournalSummary]'\""`

## Governing policies

- <a id="pa-0a2d670fe0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.validate_journal_set`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b97925a8aca80025b6903f46cbed80b946de69f4f02136447dd18416446cfd20 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(journals: 'Mapping[str, bytes]') -> 'dict[str, JournalSummary]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "validate_journal_set",
  "unit": "export"
}
```
