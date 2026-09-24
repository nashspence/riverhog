# riverhog_canonical_json.require_canonical_json

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-require-canonical-json:af815dc8c7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b337e68591"></a>
- <a id="s-fa49984827"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-a92d408ff9"></a>`module`: `riverhog_canonical_json`
- <a id="s-8bace920e5"></a>`name`: `require_canonical_json`
- <a id="s-be8825c9ab"></a>`unit`: `export`

### Declared structure

- <a id="s-efbf5ae890"></a>`kind`: `"function"`
- <a id="s-dea248246d"></a>`signature`: `"\"(raw: 'bytes') -> 'JsonValue'\""`

## Governing policies

- <a id="pa-c36c45755a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.require_canonical_json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 613a5c81f9ce6f7a89777f185c677a63af47ae84803833593158a8dafeb9a6ae -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(raw: 'bytes') -> 'JsonValue'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "require_canonical_json",
  "unit": "export"
}
```

</details>
