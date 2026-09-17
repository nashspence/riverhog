# stove0_protocol.JSON_SCHEMA_DIALECT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-json-schema-dialect:a9c9cab2eb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f09848e694"></a>
- <a id="s-c15f683c7e"></a>`distribution`: `stove0-protocol`
- <a id="s-8141637d88"></a>`module`: `stove0_protocol`
- <a id="s-38687cfc7e"></a>`name`: `JSON_SCHEMA_DIALECT`
- <a id="s-dd7edc5c5b"></a>`unit`: `export`

### Declared structure

- <a id="s-6a70940d1e"></a>`kind`: `"constant"`
- <a id="s-61fa89f21a"></a>`value`: `"https://json-schema.org/draft/2020-12/schema"`

## Governing policies

- <a id="pa-ddb0a5ecaa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JSON_SCHEMA_DIALECT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41f73db8211110b787fc34a379b8a07de2da053ff7780226ec01aa0a25eeeb09 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "https://json-schema.org/draft/2020-12/schema"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JSON_SCHEMA_DIALECT",
  "unit": "export"
}
```

</details>
