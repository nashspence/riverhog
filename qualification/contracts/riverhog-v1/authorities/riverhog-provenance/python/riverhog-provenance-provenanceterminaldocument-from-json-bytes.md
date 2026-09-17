# riverhog_provenance.ProvenanceTerminalDocument.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenanceterminaldoc-b7f71f9141:8141ee9c1b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-155a7da907"></a>
- <a id="s-624241a1a7"></a>`distribution`: `riverhog-provenance`
- <a id="s-408cab9645"></a>`module`: `riverhog_provenance`
- <a id="s-8db9902d40"></a>`name`: `from_json_bytes`
- <a id="s-b80866d48c"></a>`owner`: `riverhog_provenance.ProvenanceTerminalDocument`
- <a id="s-524a90e0b1"></a>`unit`: `member`

### Declared structure

- <a id="s-4df03b731f"></a>`kind`: `"classmethod"`
- <a id="s-d26a139a3c"></a>`signature`: `"\"(cls, content: 'bytes') -> 'ProvenanceTerminalDocument'\""`

## Maintained corroboration

### Related interface records

- [ProvenanceTerminalDocument](riverhog-provenance-provenanceterminaldocument.md)

## Governing policies

- <a id="pa-8cf68d40d4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceTerminalDocument.from_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aafe29b19bf8d15ceab48534149b554d41630aa3c781d6a187f3fe654a1e71eb -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes') -> 'ProvenanceTerminalDocument'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "from_json_bytes",
  "owner": "riverhog_provenance.ProvenanceTerminalDocument",
  "unit": "member"
}
```

</details>
