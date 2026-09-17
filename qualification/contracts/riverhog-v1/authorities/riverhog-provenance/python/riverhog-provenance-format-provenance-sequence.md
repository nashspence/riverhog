# riverhog_provenance.format_provenance_sequence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-format-provenance-sequence:45e38427f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d57539241"></a>
- <a id="s-08428d6390"></a>`distribution`: `riverhog-provenance`
- <a id="s-7be8755758"></a>`module`: `riverhog_provenance`
- <a id="s-7bd701e2cc"></a>`name`: `format_provenance_sequence`
- <a id="s-14cc2c3445"></a>`unit`: `export`

### Declared structure

- <a id="s-7ebf8d1ad7"></a>`kind`: `"function"`
- <a id="s-9f5f0282dd"></a>`signature`: `"\"(value: 'int') -> 'str'\""`

## Governing policies

- <a id="pa-2f730ff6ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — [packages/riverhog-provenance/src/riverhog\_provenance/\_\_init\_\_.py](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_provenance.format_provenance_sequence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0895a675846019caedb3c0db6b4b57ba3dd998558aac114db13c13b0b5b010e7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'int') -> 'str'\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "format_provenance_sequence",
  "unit": "export"
}
```

</details>
