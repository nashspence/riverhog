# riverhog_canonical_json.SAFE_INTEGER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-safe-integer:9ed2e9700f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9df5164969"></a>
- <a id="s-e87296cec2"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-61a6a8ee39"></a>`module`: `riverhog_canonical_json`
- <a id="s-c682d8f9ba"></a>`name`: `SAFE_INTEGER`
- <a id="s-7b6282cb87"></a>`unit`: `export`

### Declared structure

- <a id="s-d9c5e80c5f"></a>`kind`: `"constant"`
- <a id="s-96c9650496"></a>`value`: `9007199254740991`

## Governing policies

- <a id="pa-27b8184fbe"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.SAFE_INTEGER`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88c89365575755a19f2aef3887b6d79a0c5d7a2f505069dbaa6f5243a753e023 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 9007199254740991
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "SAFE_INTEGER",
  "unit": "export"
}
```

</details>
