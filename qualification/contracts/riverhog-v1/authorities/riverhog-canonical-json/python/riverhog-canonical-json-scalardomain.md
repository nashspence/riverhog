# riverhog_canonical_json.ScalarDomain

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-scalardomain:b69ec4aa34 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-097472cf3e"></a>
- <a id="s-78ae20dd7d"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-109b9425ec"></a>`module`: `riverhog_canonical_json`
- <a id="s-40d0f6cdab"></a>`name`: `ScalarDomain`
- <a id="s-688459105d"></a>`unit`: `export`

### Declared structure

- <a id="s-6a47a086e9"></a>`kind`: `"type-alias"`
- <a id="s-fb7cee772c"></a>`value`: `"typing.Literal['sequence63', 'sequence256', 'nonnegative']"`

## Governing policies

- <a id="pa-8456896be8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.ScalarDomain`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 261f093a34b5d73108bc80ec27d006f7b5cf1fa6980958b49f3977fd3b46ffec -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['sequence63', 'sequence256', 'nonnegative']"
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "ScalarDomain",
  "unit": "export"
}
```

</details>
