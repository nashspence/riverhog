# riverhog_provenance.binding_segment_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-binding-segment-bytes:4c45f6b4f2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b7d9e397d"></a>
- <a id="s-d6fffbc286"></a>`distribution`: `riverhog-provenance`
- <a id="s-df503f0f95"></a>`module`: `riverhog_provenance`
- <a id="s-a25d5ca052"></a>`name`: `binding_segment_bytes`
- <a id="s-c8bd6d56c1"></a>`unit`: `export`

### Declared structure

- <a id="s-5066b67eed"></a>`kind`: `"function"`
- <a id="s-07bfe56d6b"></a>`signature`: `"\"(*, first_file_order: 'int', files: 'list[Mapping[str, object]]') -> 'bytes'\""`

## Governing policies

- <a id="pa-96ffa652e0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.binding_segment_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc6a672ad18d071ccc36fb33dbe983aae401a879b16b5ca5a6c8bf9ca4e888bb -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, first_file_order: 'int', files: 'list[Mapping[str, object]]') -> 'bytes'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "binding_segment_bytes",
  "unit": "export"
}
```

</details>
