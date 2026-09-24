# riverhog_canonical_json.parse_identity_json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-parse-identity-json:4cca707870 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc9d49222e"></a>
- <a id="s-6f810caea2"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-5262359c31"></a>`module`: `riverhog_canonical_json`
- <a id="s-2d29dc178c"></a>`name`: `parse_identity_json`
- <a id="s-614114e81e"></a>`unit`: `export`

### Declared structure

- <a id="s-16a45b83e0"></a>`kind`: `"function"`
- <a id="s-c62bf683d2"></a>`signature`: `"\"(raw: 'bytes') -> 'JsonValue'\""`

## Governing policies

- <a id="pa-e2831f4d1a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.parse_identity_json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e8813909157515249c5c8d40738167a8d8495e5b72e5a4b01ca9eb7c978b851 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'bytes') -> 'JsonValue'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "parse_identity_json",
  "unit": "export"
}
```

</details>
