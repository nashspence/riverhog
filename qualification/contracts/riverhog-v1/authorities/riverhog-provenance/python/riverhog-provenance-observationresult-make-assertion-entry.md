# riverhog_provenance.ObservationResult.make_assertion_entry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-observationresult-mak-b6b64eaf3d:46d0c2844f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aba7712c9a"></a>
- <a id="s-d4ff09d3ba"></a>`distribution`: `riverhog-provenance`
- <a id="s-2bab3bb6df"></a>`module`: `riverhog_provenance`
- <a id="s-6d2c809a44"></a>`name`: `make_assertion_entry`
- <a id="s-eb3586f62c"></a>`owner`: `riverhog_provenance.ObservationResult`
- <a id="s-b0827fe298"></a>`unit`: `member`

### Declared structure

- <a id="s-f7d1a6f305"></a>`kind`: `"method"`
- <a id="s-77bc0d59d1"></a>`signature`: `"\"(self, *, journal_id: 'str', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', previous_sequence: 'int \| None' = None, entry_id: 'str \| None' = None, recorded_at: 'str \| None' = None, recorded_by_agent_id: 'str \| None' = None, notes: 'Sequence[str]' = (), omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""`

## Maintained corroboration

### Related interface records

- [ObservationResult](riverhog-provenance-observationresult.md)

## Governing policies

- <a id="pa-b54c91b202"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ObservationResult.make_assertion_entry`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1f9b1a17b93d230522ecb9212ad3dfea0e1d9f505c08d699acbd6005d0a61d6e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, journal_id: 'str', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', previous_sequence: 'int | None' = None, entry_id: 'str | None' = None, recorded_at: 'str | None' = None, recorded_by_agent_id: 'str | None' = None, notes: 'Sequence[str]' = (), omit_object_ids: 'Sequence[str]' = ()) -> 'JsonObject'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "make_assertion_entry",
  "owner": "riverhog_provenance.ObservationResult",
  "unit": "member"
}
```

</details>
