# riverhog_provenance.validate_journal_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-validate-journal-set:ecedc9bc4d -->

Exact externally visible contract owned by this contract element.

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

- <a id="pa-0a2d670fe0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.validate_journal_set`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
