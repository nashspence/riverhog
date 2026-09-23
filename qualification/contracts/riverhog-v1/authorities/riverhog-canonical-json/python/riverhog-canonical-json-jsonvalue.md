# riverhog_canonical_json.JsonValue

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-canonical-json:riverhog-canonical-json-jsonvalue:13b7cf30d3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-canonical-json](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7f39ac9a20"></a>
- <a id="s-0db0654775"></a>`distribution`: `riverhog-canonical-json`
- <a id="s-4644a25f13"></a>`module`: `riverhog_canonical_json`
- <a id="s-abc4b5b7ba"></a>`name`: `JsonValue`
- <a id="s-d8686d7c01"></a>`unit`: `export`

### Declared structure

- <a id="s-577b7928c2"></a>`kind`: `"type-alias"`
- <a id="s-bb37f39237"></a>`value`: `"None \| bool \| int \| float \| str \| list[JsonValue] \| dict[str, JsonValue]"`

## Governing policies

- <a id="pa-c6c8a74074"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-canonical-json:riverhog_canonical_json](../../../evidence/sources/authorities.md#src-30f59d1ecd) — [packages/riverhog-canonical-json/src/riverhog\_canonical\_json/\_\_init\_\_.py](../../../../../../packages/riverhog-canonical-json/src/riverhog_canonical_json/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_canonical_json.JsonValue`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d10b2a15fc554a06efc41fa6b05a80efc99d4daf21e0c22404dfe9b860356318 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]"
  },
  "distribution": "riverhog-canonical-json",
  "module": "riverhog_canonical_json",
  "name": "JsonValue",
  "unit": "export"
}
```

</details>
