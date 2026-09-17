# riverhog_provenance.SIDECAR_SUFFIX

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-sidecar-suffix:02e334bd4b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7475bc97a5"></a>
- <a id="s-1e13f12eda"></a>`distribution`: `riverhog-provenance`
- <a id="s-5ee1f6074e"></a>`module`: `riverhog_provenance`
- <a id="s-d96a2fb4a4"></a>`name`: `SIDECAR_SUFFIX`
- <a id="s-1a8bf2836f"></a>`unit`: `export`

### Declared structure

- <a id="s-4baa82ce5a"></a>`kind`: `"constant"`
- <a id="s-df03a3e475"></a>`value`: `".riverhog-provenance.json-seq"`

## Governing policies

- <a id="pa-0509fdb6d4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.SIDECAR_SUFFIX`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aef0828420ee8e0c8e04c7a8840ac6630d4247c6e6b4f2a26c9a31e9cd461c81 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": ".riverhog-provenance.json-seq"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "SIDECAR_SUFFIX",
  "unit": "export"
}
```

</details>
