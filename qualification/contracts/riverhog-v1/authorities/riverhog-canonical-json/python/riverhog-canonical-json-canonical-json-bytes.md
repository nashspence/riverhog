# riverhog_canonical_json.canonical_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-canonical-json-bytes:9bbf5848b4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b78281fa5"></a>
- <a id="s-2618fa266a"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-b7377f76f2"></a>`module`: `riverhog_canonical_json`
- <a id="s-0183d8ce1b"></a>`name`: `canonical_json_bytes`
- <a id="s-d68591593d"></a>`unit`: `export`

### Declared structure

- <a id="s-e0c5226b0d"></a>`kind`: `"function"`
- <a id="s-fb4407cc45"></a>`signature`: `"\"(value: 'object') -> 'bytes'\""`

## Governing policies

- <a id="pa-e9b33188e1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.canonical_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56a7718075a107df43a7329b2b440cc28c071920f80e5e88d001485beb77d4c3 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'object') -> 'bytes'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "canonical_json_bytes",
  "unit": "export"
}
```

</details>
