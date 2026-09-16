# riverhog_age.iter_decrypt_age_scrypt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-iter-decrypt-age-scrypt:76c53c8641 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6eb10578b3"></a>
- <a id="s-8a6723a419"></a>`distribution`: `riverhog-age`
- <a id="s-9c1b0f14a7"></a>`module`: `riverhog_age`
- <a id="s-c5de2d455e"></a>`name`: `iter_decrypt_age_scrypt`
- <a id="s-390afcaf3f"></a>`unit`: `export`

### Declared structure

- <a id="s-04a619e49b"></a>`kind`: `"function"`
- <a id="s-4cb1e8c7c0"></a>`signature`: `"\"(chunks: 'Iterable[bytes]', passphrase: 'str \| bytes', *, scrypt_maxmem: 'int \| None' = None) -> 'Iterator[bytes]'\""`

## Governing policies

- <a id="pa-8306fe18d9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — `packages/riverhog-age/src/riverhog_age/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_age.iter_decrypt_age_scrypt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5387c7762101cc524afdf08dcdd4d01f180bad153e8392c9654710061c7b67a9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(chunks: 'Iterable[bytes]', passphrase: 'str | bytes', *, scrypt_maxmem: 'int | None' = None) -> 'Iterator[bytes]'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "iter_decrypt_age_scrypt",
  "unit": "export"
}
```

</details>
