# riverhog_age.split_plaintext_chunks

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-split-plaintext-chunks:45fdfca5eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a16232180"></a>
- <a id="s-1a01c3d942"></a>`distribution`: `riverhog-age`
- <a id="s-898849a2e6"></a>`module`: `riverhog_age`
- <a id="s-15bb68d7a3"></a>`name`: `split_plaintext_chunks`
- <a id="s-afab84ff01"></a>`unit`: `export`

### Declared structure

- <a id="s-db42642c0f"></a>`kind`: `"function"`
- <a id="s-d443290e26"></a>`signature`: `"\"(plaintext_size: 'int') -> 'list[tuple[int, int, int, bool]]'\""`

## Governing policies

- <a id="pa-d97c03a7c0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.split_plaintext_chunks`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 67d8adea4dcfa37e5c87f2d1c44c8df08eb065c7c291d073e1e39e3947f185c1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plaintext_size: 'int') -> 'list[tuple[int, int, int, bool]]'\""
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "split_plaintext_chunks",
  "unit": "export"
}
```

</details>
