# riverhog_protocol.COLLECTION_DESCRIPTION_RELATIVE_PATH

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-description-e1e52faaf9:6df7cc8825 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9632e2a0d3"></a>
- <a id="s-997684c983"></a>`distribution`: `riverhog-protocol`
- <a id="s-f0d1986802"></a>`module`: `riverhog_protocol`
- <a id="s-87fd7d8555"></a>`name`: `COLLECTION_DESCRIPTION_RELATIVE_PATH`
- <a id="s-fc5bb0a0af"></a>`unit`: `export`

### Declared structure

- <a id="s-ac7a796e1a"></a>`kind`: `"constant"`
- <a id="s-2e4db676b3"></a>`value`: `"description.json.age"`

## Governing policies

- <a id="pa-71869e8101"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.COLLECTION_DESCRIPTION_RELATIVE_PATH`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d907852f5607a0bdc7022c15dcf68ad622e15d849dfff79a434e1a8a0adcd001 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "description.json.age"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "COLLECTION_DESCRIPTION_RELATIVE_PATH",
  "unit": "export"
}
```

</details>
