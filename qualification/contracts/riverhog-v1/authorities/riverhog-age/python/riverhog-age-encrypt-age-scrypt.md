# riverhog_age.encrypt_age_scrypt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-encrypt-age-scrypt:5533059246 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b99baa14e"></a>
- <a id="s-2653f8c0e0"></a>`distribution`: `riverhog-age`
- <a id="s-85003d2572"></a>`module`: `riverhog_age`
- <a id="s-d81f934645"></a>`name`: `encrypt_age_scrypt`
- <a id="s-1f5ded5dad"></a>`unit`: `export`

### Declared structure

- <a id="s-a51957087b"></a>`kind`: `"function"`
- <a id="s-17c0746c1d"></a>`signature`: `"\"(plaintext: 'bytes', passphrase: 'str \| bytes', *, log_n: 'int' = 18, scrypt_maxmem: 'int \| None' = None) -> 'bytes'\""`

## Governing policies

- <a id="pa-67c4d82470"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.encrypt_age_scrypt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fbe321734eac0e52ebfdf62ec6ee187b9333d9cdb0c8609aaf8e28893c086fdd -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plaintext: 'bytes', passphrase: 'str | bytes', *, log_n: 'int' = 18, scrypt_maxmem: 'int | None' = None) -> 'bytes'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "encrypt_age_scrypt",
  "unit": "export"
}
```

</details>
