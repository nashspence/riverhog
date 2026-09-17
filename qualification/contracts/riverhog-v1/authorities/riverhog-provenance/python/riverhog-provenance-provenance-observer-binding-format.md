# riverhog_provenance.PROVENANCE_OBSERVER_BINDING_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-observer-b-0256a63ac9:de04955cb9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-00a40bb0c7"></a>
- <a id="s-33df43b4d3"></a>`distribution`: `riverhog-provenance`
- <a id="s-f8170c2098"></a>`module`: `riverhog_provenance`
- <a id="s-d50252a850"></a>`name`: `PROVENANCE_OBSERVER_BINDING_FORMAT`
- <a id="s-5688bb0630"></a>`unit`: `export`

### Declared structure

- <a id="s-5f2b1c4e45"></a>`kind`: `"constant"`
- <a id="s-115814e877"></a>`value`: `"riverhog-provenance-observer-binding/v1"`

## Governing policies

- <a id="pa-655a6490b3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_OBSERVER_BINDING_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 575b78033b722953f7e991fea60287050698a4604ee4790e66fb72dc8a5cee94 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-observer-binding/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_OBSERVER_BINDING_FORMAT",
  "unit": "export"
}
```

</details>
