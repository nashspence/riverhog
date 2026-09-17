# riverhog_age.DEFAULT_CHUNKS_PER_AGE_UNIT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-age:riverhog-age-default-chunks-per-age-unit:bec625b904 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-age](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-23692dc3c8"></a>
- <a id="s-b052f73bc8"></a>`distribution`: `riverhog-age`
- <a id="s-478980718a"></a>`module`: `riverhog_age`
- <a id="s-a19a4ced5b"></a>`name`: `DEFAULT_CHUNKS_PER_AGE_UNIT`
- <a id="s-0bdab5fc9f"></a>`unit`: `export`

### Declared structure

- <a id="s-7bbab37373"></a>`kind`: `"constant"`
- <a id="s-fbab8c5bc7"></a>`value`: `1024`

## Governing policies

- <a id="pa-78021a400f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-age:riverhog_age](../../../evidence/sources/authorities.md#src-a842e50b8b) — [packages/riverhog-age/src/riverhog\_age/\_\_init\_\_.py](../../../../../../packages/riverhog-age/src/riverhog_age/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_age.DEFAULT_CHUNKS_PER_AGE_UNIT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cff9f2411734d704f2a031874c331014bfd8d24dd9fe53f06586faab9f5e8291 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 1024
  },
  "distribution": "riverhog-age",
  "module": "riverhog_age",
  "name": "DEFAULT_CHUNKS_PER_AGE_UNIT",
  "unit": "export"
}
```

</details>
