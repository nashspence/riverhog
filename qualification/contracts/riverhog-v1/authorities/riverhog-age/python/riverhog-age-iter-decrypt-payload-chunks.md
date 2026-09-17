# riverhog_age.iter_decrypt_payload_chunks

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-iter-decrypt-payload-chunks:1a9efeed8c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5a73dc7177"></a>
- <a id="s-a2ab22e7e1"></a>`distribution`: `riverhog-age`
- <a id="s-f0ce4bf5fd"></a>`module`: `riverhog_age`
- <a id="s-9401127bf4"></a>`name`: `iter_decrypt_payload_chunks`
- <a id="s-6fb26900a1"></a>`unit`: `export`

### Declared structure

- <a id="s-bb5add9821"></a>`kind`: `"function"`
- <a id="s-7a10b37a3b"></a>`signature`: `"\"(file_key: 'bytes', payload_nonce: 'bytes', ciphertext_chunks: 'Iterable[bytes]') -> 'Iterator[bytes]'\""`

## Governing policies

- <a id="pa-27276cedb1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.iter_decrypt_payload_chunks`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 402fbbbc602d8adadb29fd9cfd5524b03ad1c6745cf8939c97e5c2d6b39e06e5 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(file_key: 'bytes', payload_nonce: 'bytes', ciphertext_chunks: 'Iterable[bytes]') -> 'Iterator[bytes]'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "iter_decrypt_payload_chunks",
  "unit": "export"
}
```

</details>
