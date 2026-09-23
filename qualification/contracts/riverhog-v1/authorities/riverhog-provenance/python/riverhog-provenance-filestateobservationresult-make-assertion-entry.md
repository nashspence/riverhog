# riverhog_provenance.FileStateObservationResult.make_assertion_entry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-filestateobservationr-65f5040a13:9d26aef06f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f83e16cb9c"></a>
- <a id="s-d28718a555"></a>`distribution`: `riverhog-provenance`
- <a id="s-eab833781e"></a>`module`: `riverhog_provenance`
- <a id="s-d87269c758"></a>`name`: `make_assertion_entry`
- <a id="s-7956112676"></a>`owner`: `riverhog_provenance.FileStateObservationResult`
- <a id="s-5c2406f42a"></a>`unit`: `member`

### Declared structure

- <a id="s-bf0b6a29eb"></a>`kind`: `"method"`
- <a id="s-7b376b2581"></a>`signature`: `"\"(self, *, journal_id: 'str', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', previous_sequence: 'int \| None' = None, entry_id: 'str \| None' = None, recorded_at: 'str \| None' = None, recorded_by_agent_id: 'str \| None' = None, notes: 'Sequence[str]' = (), omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""`

## Maintained corroboration

### Related interface records

- [FileStateObservationResult](riverhog-provenance-filestateobservationresult.md)

## Governing policies

- <a id="pa-1dc7e7b4b6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.FileStateObservationResult.make_assertion_entry`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e599c73c90484dd15e07cdd74237df8b1185ff7f1e5f886f0f155050e101dd14 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, journal_id: 'str', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', previous_sequence: 'int | None' = None, entry_id: 'str | None' = None, recorded_at: 'str | None' = None, recorded_by_agent_id: 'str | None' = None, notes: 'Sequence[str]' = (), omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "make_assertion_entry",
  "owner": "riverhog_provenance.FileStateObservationResult",
  "unit": "member"
}
```

</details>
