# riverhog_provenance.update_ordered_volume_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-update-ordered-volume-commitment:f5ea1f3748 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1d0440e727"></a>
- <a id="s-84f6e2296d"></a>`distribution`: `riverhog-provenance`
- <a id="s-686772fbc1"></a>`module`: `riverhog_provenance`
- <a id="s-3eb33613ea"></a>`name`: `update_ordered_volume_commitment`
- <a id="s-0e2505a840"></a>`unit`: `export`

### Declared structure

- <a id="s-54250eb96a"></a>`kind`: `"function"`
- <a id="s-9154ad8d0f"></a>`signature`: `"\"(digest: 'Any', document: 'ProvenanceSequenceDocument') -> 'None'\""`

## Governing policies

- <a id="pa-fb95066b74"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.update_ordered_volume_commitment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37f51423c9b50f4686e3a5fbf5a2c809fa67e6052236062a27d3b3f0e4948f85 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'Any', document: 'ProvenanceSequenceDocument') -> 'None'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "update_ordered_volume_commitment",
  "unit": "export"
}
```

</details>
