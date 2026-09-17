# riverhog_provenance.create_derivative_journal_from_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-create-derivative-jou-4dfa40665c:a348792a72 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-27818f08d1"></a>
- <a id="s-f7cc88d2eb"></a>`distribution`: `riverhog-provenance`
- <a id="s-a5918c8a3b"></a>`module`: `riverhog_provenance`
- <a id="s-9d6b0a73a6"></a>`name`: `create_derivative_journal_from_identity`
- <a id="s-8e8aff3550"></a>`unit`: `export`

### Declared structure

- <a id="s-729b90c7c7"></a>`kind`: `"function"`
- <a id="s-37c71b4617"></a>`signature`: `"\"(*, relative_path: 'str', byte_count: 'int', sha256: 'str', source_journals: 'Sequence[bytes]', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', journal_id: 'str \| None' = None, derivation_kind: 'str' = 'transformation', evidence: 'Sequence[Mapping[str, Any]]' = ()) -> 'bytes'\""`

## Governing policies

- <a id="pa-15258562d4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.create_derivative_journal_from_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d675bcf703ac3077cdaaaebc0e6917aebacdff591f179858f7a53fa62aa15593 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, relative_path: 'str', byte_count: 'int', sha256: 'str', source_journals: 'Sequence[bytes]', agent_name: 'str', agent_version: 'str', event_label: 'str', started_at: 'str', ended_at: 'str', journal_id: 'str | None' = None, derivation_kind: 'str' = 'transformation', evidence: 'Sequence[Mapping[str, Any]]' = ()) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "create_derivative_journal_from_identity",
  "unit": "export"
}
```

</details>
