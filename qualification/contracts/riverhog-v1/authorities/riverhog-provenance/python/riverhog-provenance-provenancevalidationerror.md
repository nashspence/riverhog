# riverhog_provenance.ProvenanceValidationError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenancevalidationerror:59e6ff9b24 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6609655e2"></a>
- <a id="s-9f34b82c3e"></a>`distribution`: `riverhog-provenance`
- <a id="s-8dd8cee3b1"></a>`module`: `riverhog_provenance`
- <a id="s-3091f9bed8"></a>`name`: `ProvenanceValidationError`
- <a id="s-ba7e97af60"></a>`unit`: `export`

### Declared structure

- <a id="s-833fed6c1c"></a>`kind`: `"class"`
- <a id="s-05dbb64642"></a>`signature`: `"unavailable"`

## Governing policies

- <a id="pa-efa764b2f5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.ProvenanceValidationError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3af6b39201a1a223ca6b03e5e0426604c6e26f6e036951e4bdf6bb32018d6d1d -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ProvenanceValidationError",
  "unit": "export"
}
```

</details>
