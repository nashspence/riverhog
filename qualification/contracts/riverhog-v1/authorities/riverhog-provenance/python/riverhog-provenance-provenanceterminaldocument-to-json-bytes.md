# riverhog_provenance.ProvenanceTerminalDocument.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenanceterminaldoc-3305411c0d:14ced98650 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-53c2047dd8"></a>
- <a id="s-e4fab62c27"></a>`distribution`: `riverhog-provenance`
- <a id="s-277e0736d6"></a>`module`: `riverhog_provenance`
- <a id="s-ac8e8eca9f"></a>`name`: `to_json_bytes`
- <a id="s-656e405963"></a>`owner`: `riverhog_provenance.ProvenanceTerminalDocument`
- <a id="s-0a298a9edf"></a>`unit`: `member`

### Declared structure

- <a id="s-044f062a9a"></a>`kind`: `"method"`
- <a id="s-b30638cd5b"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ProvenanceTerminalDocument](riverhog-provenance-provenanceterminaldocument.md)

## Governing policies

- <a id="pa-4115e34f6c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceTerminalDocument.to_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 30458276b082393448d82e5be1cfe64e44ad5a69f67dda0ef0b039b0e344d697 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "to_json_bytes",
  "owner": "riverhog_provenance.ProvenanceTerminalDocument",
  "unit": "member"
}
```

</details>
