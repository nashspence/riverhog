# riverhog_provenance.UnstableFileError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-unstablefileerror:ed8483d86e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f533467827"></a>
- <a id="s-0616118234"></a>`distribution`: `riverhog-provenance`
- <a id="s-4134d61ff2"></a>`module`: `riverhog_provenance`
- <a id="s-122a9c9685"></a>`name`: `UnstableFileError`
- <a id="s-4c627b4561"></a>`unit`: `export`

### Declared structure

- <a id="s-868cbbe632"></a>`kind`: `"class"`
- <a id="s-4cc87acfdc"></a>`signature`: `"unavailable"`

## Governing policies

- <a id="pa-0159d97c64"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.UnstableFileError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3f2fd868a6ef9d0eccf6fe05c1053438a0b88964f0abed0a85e2c1dbbdc646e -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "UnstableFileError",
  "unit": "export"
}
```

</details>
