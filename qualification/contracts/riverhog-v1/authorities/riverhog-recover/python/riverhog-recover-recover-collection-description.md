# riverhog_recover.recover_collection_description

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-recover:riverhog-recover-recover-collection-description:a817e70a5e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d9f3694a6b"></a>
- <a id="s-8c95a83c36"></a>`distribution`: `riverhog-recover`
- <a id="s-f4dfeb7db0"></a>`module`: `riverhog_recover`
- <a id="s-bb2412afa8"></a>`name`: `recover_collection_description`
- <a id="s-183c9274f3"></a>`unit`: `export`

### Declared structure

- <a id="s-1b55843181"></a>`kind`: `"function"`
- <a id="s-6bb56a4bb0"></a>`signature`: `"\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'CollectionDescriptionDocument \| None'\""`

## Governing policies

- <a id="pa-af95514be9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-recover:riverhog_recover](../../../evidence/sources/authorities.md#src-dbfe6c5e2e) — [reference/riverhog/recovery/src/riverhog\_recover/\_\_init\_\_.py](../../../../../../reference/riverhog/recovery/src/riverhog_recover/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_recover.recover_collection_description`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 465c3d5e2be05591d7346167224969ee7dd20008e5c88697f25a9b5cfebc0b80 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(archive_dir: 'Path', *, passphrases: 'Mapping[str, str]', age_command: 'str' = 'age') -> 'CollectionDescriptionDocument | None'\""
  },
  "distribution": "riverhog-recover",
  "module": "riverhog_recover",
  "name": "recover_collection_description",
  "unit": "export"
}
```

</details>
