# riverhog_canonical_json.canonical_json_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-canonical-json-sha256:3cf8a70577 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b69244cd26"></a>
- <a id="s-e83f501d2a"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-8c2034c71f"></a>`module`: `riverhog_canonical_json`
- <a id="s-75e223c25d"></a>`name`: `canonical_json_sha256`
- <a id="s-0d5a7d9b1e"></a>`unit`: `export`

### Declared structure

- <a id="s-5ad7a7f43d"></a>`kind`: `"function"`
- <a id="s-ca4337208f"></a>`signature`: `"\"(value: 'object') -> 'str'\""`

## Governing policies

- <a id="pa-cdb5b2bbe8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.canonical_json_sha256`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f54613167bd9030ede346a477032fe609d2b26019fd0c9831674cfdeac06e48a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'str'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "canonical_json_sha256",
  "unit": "export"
}
```

</details>
