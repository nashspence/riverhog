# riverhog_age.age_chunk_count_for_plaintext_len

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-age-chunk-count-for-plaintext-len:e85cb4b070 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8e6372907"></a>
- <a id="s-acf588c673"></a>`distribution`: `riverhog-age`
- <a id="s-6752a2f2b2"></a>`module`: `riverhog_age`
- <a id="s-75ef869235"></a>`name`: `age_chunk_count_for_plaintext_len`
- <a id="s-fb56f76d1b"></a>`unit`: `export`

### Declared structure

- <a id="s-3e5317a8fd"></a>`kind`: `"function"`
- <a id="s-af151ea5e2"></a>`signature`: `"\"(plaintext_size: 'int') -> 'int'\""`

## Governing policies

- <a id="pa-3489ac60b2"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.age_chunk_count_for_plaintext_len`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 31bf0c65b852e10dd291c751074d569feb9932f9025e6a83a78f428e13ce8311 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plaintext_size: 'int') -> 'int'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "age_chunk_count_for_plaintext_len",
  "unit": "export"
}
```

</details>
