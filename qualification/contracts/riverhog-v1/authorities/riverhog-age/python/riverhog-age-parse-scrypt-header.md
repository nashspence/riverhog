# riverhog_age.parse_scrypt_header

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-parse-scrypt-header:e03679858f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f9b8142a5e"></a>
- <a id="s-7e71acdced"></a>`distribution`: `riverhog-age`
- <a id="s-897c914e0f"></a>`module`: `riverhog_age`
- <a id="s-5bb0703f5b"></a>`name`: `parse_scrypt_header`
- <a id="s-2eda654611"></a>`unit`: `export`

### Declared structure

- <a id="s-d029cf362e"></a>`kind`: `"function"`
- <a id="s-2c6c5b2154"></a>`signature`: `"\"(header: 'bytes') -> 'ParsedScryptHeader'\""`

## Governing policies

- <a id="pa-e13cb9b0b8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.parse_scrypt_header`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08aba43d348e3a451c357c1a29537cb23c306b9e75173d1cdf676f5c9773c6db -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(header: 'bytes') -> 'ParsedScryptHeader'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "parse_scrypt_header",
  "unit": "export"
}
```

</details>
