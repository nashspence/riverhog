# riverhog_provenance.bounded_binding_segment_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-bounded-binding-segment-bytes:0601301cef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-279594dfa2"></a>
- <a id="s-ff095572d5"></a>`distribution`: `riverhog-provenance`
- <a id="s-f18d494915"></a>`module`: `riverhog_provenance`
- <a id="s-e6555e531e"></a>`name`: `bounded_binding_segment_bytes`
- <a id="s-1ede51dc77"></a>`unit`: `export`

### Declared structure

- <a id="s-ecbca4ec5a"></a>`kind`: `"function"`
- <a id="s-070b5007c2"></a>`signature`: `"\"(*, first_file_order: 'int', files: 'list[Mapping[str, object]]') -> 'tuple[bytes, int]'\""`

## Governing policies

- <a id="pa-ce8d076b2d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.bounded_binding_segment_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4efcb9f091f97bdac3a18701fb1220b125a174c8c567541d779169d9dfad7677 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, first_file_order: 'int', files: 'list[Mapping[str, object]]') -> 'tuple[bytes, int]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "bounded_binding_segment_bytes",
  "unit": "export"
}
```
