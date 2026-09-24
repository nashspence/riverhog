# riverhog_provenance.PROVENANCE_BINDING_SEGMENT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-binding-se-46c6441a95:9b6b6a7511 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-463e9e073b"></a>
- <a id="s-ea7beac458"></a>`distribution`: `riverhog-provenance`
- <a id="s-7610f653b1"></a>`module`: `riverhog_provenance`
- <a id="s-40cc5a8477"></a>`name`: `PROVENANCE_BINDING_SEGMENT_FORMAT`
- <a id="s-154fc3aaab"></a>`unit`: `export`

### Declared structure

- <a id="s-d1a9d20e80"></a>`kind`: `"constant"`
- <a id="s-362af19892"></a>`value`: `"riverhog-provenance-bindings/v1"`

## Governing policies

- <a id="pa-e7bdd9b1c0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_BINDING_SEGMENT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86c2b72a7b12ffaa1511bf0329b7c9c6547c060116ea283165410f038a3de83a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-bindings/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_BINDING_SEGMENT_FORMAT",
  "unit": "export"
}
```

</details>
