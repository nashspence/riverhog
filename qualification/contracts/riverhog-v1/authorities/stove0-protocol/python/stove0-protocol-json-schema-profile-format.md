# stove0_protocol.JSON_SCHEMA_PROFILE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-json-schema-profile-format:eef138d0b1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9713aaaeab"></a>
- <a id="s-cb2ae78f44"></a>`distribution`: `stove0-protocol`
- <a id="s-93705b9ee8"></a>`module`: `stove0_protocol`
- <a id="s-69621c8509"></a>`name`: `JSON_SCHEMA_PROFILE_FORMAT`
- <a id="s-7b2be2ed1b"></a>`unit`: `export`

### Declared structure

- <a id="s-6cf34fe0a2"></a>`kind`: `"constant"`
- <a id="s-543376daac"></a>`value`: `"stove0-json-schema-profile/v1"`

## Governing policies

- <a id="pa-324626c50c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JSON_SCHEMA_PROFILE_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1bd4efed4349f6472d09151e4f5f87b5ec8644ae8a1e966de132d919574c4cd5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-json-schema-profile/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JSON_SCHEMA_PROFILE_FORMAT",
  "unit": "export"
}
```

</details>
