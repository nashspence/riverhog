# riverhog_provenance.PROVENANCE_ROOT_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-provenance-root-format:693675843a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-15c0682bd3"></a>
- <a id="s-397eb3103c"></a>`distribution`: `riverhog-provenance`
- <a id="s-390274ea8d"></a>`module`: `riverhog_provenance`
- <a id="s-ea5bbb5d3d"></a>`name`: `PROVENANCE_ROOT_FORMAT`
- <a id="s-6686f9929c"></a>`unit`: `export`

### Declared structure

- <a id="s-a3f6fc2c9a"></a>`kind`: `"constant"`
- <a id="s-055b4adb59"></a>`value`: `"riverhog-provenance-root/v1"`

## Governing policies

- <a id="pa-a0d275f03a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources/authorities.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.PROVENANCE_ROOT_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 195330e37c8d45e62940875c64c8b9cfcd3afecabe07e347ab890ba430f13d79 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-provenance-root/v1"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PROVENANCE_ROOT_FORMAT",
  "unit": "export"
}
```

</details>
