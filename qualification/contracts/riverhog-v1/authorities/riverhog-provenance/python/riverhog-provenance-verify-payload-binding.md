# riverhog_provenance.verify_payload_binding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-verify-payload-binding:36e536f8f5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1035914fb1"></a>
- <a id="s-b891eae588"></a>`distribution`: `riverhog-provenance`
- <a id="s-bab2d97f9a"></a>`module`: `riverhog_provenance`
- <a id="s-9efbbc06e0"></a>`name`: `verify_payload_binding`
- <a id="s-540da812c9"></a>`unit`: `export`

### Declared structure

- <a id="s-1cea367aef"></a>`kind`: `"function"`
- <a id="s-16b39bf0d2"></a>`signature`: `"\"(summary: 'JournalSummary', *, path: 'str', byte_count: 'int', sha256: 'str') -> 'None'\""`

## Governing policies

- <a id="pa-626007eba4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.verify_payload_binding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1558463c3d96a9caacc9619ac63025e8274ebb6dbd58238a374c34dfda759ad2 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(summary: 'JournalSummary', *, path: 'str', byte_count: 'int', sha256: 'str') -> 'None'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "verify_payload_binding",
  "unit": "export"
}
```

</details>
