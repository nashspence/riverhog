# riverhog_age.plaintext_bytes_for_ciphertext_offset

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-plaintext-bytes-for-ciphertext-offset:228e7a8334 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1db72bebc5"></a>
- <a id="s-0ce1822be3"></a>`distribution`: `riverhog-age`
- <a id="s-18b90c0857"></a>`module`: `riverhog_age`
- <a id="s-d1446d10d1"></a>`name`: `plaintext_bytes_for_ciphertext_offset`
- <a id="s-70cd7e0a70"></a>`unit`: `export`

### Declared structure

- <a id="s-3f99d2b429"></a>`kind`: `"function"`
- <a id="s-110611364c"></a>`signature`: `"\"(*, state: 'UploadState \| bytes \| str \| Mapping[str, object]', plaintext_bytes: 'int', ciphertext_bytes: 'int', ciphertext_offset: 'int') -> 'int'\""`

## Governing policies

- <a id="pa-0cb20f7b51"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.plaintext_bytes_for_ciphertext_offset`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e2d8dfc9fb97eb700dfe15912c473bc1a169ee700f09d4722fab842349c3091 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, state: 'UploadState | bytes | str | Mapping[str, object]', plaintext_bytes: 'int', ciphertext_bytes: 'int', ciphertext_offset: 'int') -> 'int'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "plaintext_bytes_for_ciphertext_offset",
  "unit": "export"
}
```

</details>
