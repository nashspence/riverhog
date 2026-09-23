# riverhog_canonical_json.scalar_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-scalar-schema:55d606b231 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff8c4b8be5"></a>
- <a id="s-f397289e48"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-c521786146"></a>`module`: `riverhog_canonical_json`
- <a id="s-a1dcf42117"></a>`name`: `scalar_schema`
- <a id="s-5a1b96dbfe"></a>`unit`: `export`

### Declared structure

- <a id="s-b5b7decfd1"></a>`kind`: `"function"`
- <a id="s-7d672ca68e"></a>`signature`: `"\"(domain: 'ScalarDomain') -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-80753b4ef6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.scalar_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82f34781b5a0db3a0799d0656a8b1b63558e900128456a8e854ea0eeb70053db -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(domain: 'ScalarDomain') -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "scalar_schema",
  "unit": "export"
}
```

</details>
