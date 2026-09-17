# riverhog_provenance.parse_binding_segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-parse-binding-segment:e6853b3363 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1856ae5d79"></a>
- <a id="s-35701a28b3"></a>`distribution`: `riverhog-provenance`
- <a id="s-9b2ec61974"></a>`module`: `riverhog_provenance`
- <a id="s-3f5639e782"></a>`name`: `parse_binding_segment`
- <a id="s-6dfeffa706"></a>`unit`: `export`

### Declared structure

- <a id="s-a6cdf657c2"></a>`kind`: `"function"`
- <a id="s-03abe2a5d1"></a>`signature`: `"\"(content: 'bytes') -> 'tuple[int, list[dict[str, object]]]'\""`

## Governing policies

- <a id="pa-ea360c0cc7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.parse_binding_segment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dc17c3e9001a46078f1e2b3a2c9987af9b98b4c9631876a8707c67b4ae776f08 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(content: 'bytes') -> 'tuple[int, list[dict[str, object]]]'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "parse_binding_segment",
  "unit": "export"
}
```

</details>
