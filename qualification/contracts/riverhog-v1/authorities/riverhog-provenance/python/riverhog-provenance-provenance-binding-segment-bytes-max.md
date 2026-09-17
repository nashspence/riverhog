# riverhog_provenance.PROVENANCE_BINDING_SEGMENT_BYTES_MAX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-binding-se-33e1ae8da3:7482aaa409 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8a85d546ee"></a>
- <a id="s-30247be402"></a>`distribution`: `riverhog-provenance`
- <a id="s-4127c575d2"></a>`module`: `riverhog_provenance`
- <a id="s-0663939546"></a>`name`: `PROVENANCE_BINDING_SEGMENT_BYTES_MAX`
- <a id="s-ed12722b73"></a>`unit`: `export`

### Declared structure

- <a id="s-ca8c381b89"></a>`kind`: `"constant"`
- <a id="s-5e9c54d74e"></a>`value`: `4194304`

## Governing policies

- <a id="pa-59bf1e0dce"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_BINDING_SEGMENT_BYTES_MAX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39bede3d11d00d991ca9050b791f9b1bd0d4387b717ac765c0da5aab76688ad2 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 4194304
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_BINDING_SEGMENT_BYTES_MAX",
  "unit": "export"
}
```

</details>
