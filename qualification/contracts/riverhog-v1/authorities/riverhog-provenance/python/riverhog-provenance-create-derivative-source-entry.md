# riverhog_provenance.create_derivative_source_entry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-create-derivative-source-entry:133a7137fa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-615ebc3c77"></a>
- <a id="s-85408b0f9d"></a>`distribution`: `riverhog-provenance`
- <a id="s-af5478e0e1"></a>`module`: `riverhog_provenance`
- <a id="s-1fe5430229"></a>`name`: `create_derivative_source_entry`
- <a id="s-adaa644021"></a>`unit`: `export`

### Declared structure

- <a id="s-0c452a28ad"></a>`kind`: `"function"`
- <a id="s-a5fb4cd0cd"></a>`signature`: `"\"(*, seed: 'DerivativeJournalSeed', references: 'Sequence[ExternalStateReference]', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', recorded_at: 'str') -> 'bytes'\""`

## Governing policies

- <a id="pa-c97a56dae1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.create_derivative_source_entry`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 386115a0dbddfd4b6400b6d7b090a3fa8bd69aa00022b11cc8a48cd4fb3720c7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, seed: 'DerivativeJournalSeed', references: 'Sequence[ExternalStateReference]', sequence: 'int', previous_entry_id: 'str', previous_entry_json_sha256: 'str', recorded_at: 'str') -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "create_derivative_source_entry",
  "unit": "export"
}
```
