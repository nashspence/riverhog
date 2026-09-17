# stove0_protocol.SHA256_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-sha256-pattern:bd5d3f406b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5c4f28fc91"></a>
- <a id="s-76df178e8f"></a>`distribution`: `stove0-protocol`
- <a id="s-a51b0c813f"></a>`module`: `stove0_protocol`
- <a id="s-c2b4b83fc8"></a>`name`: `SHA256_PATTERN`
- <a id="s-e8b99dbedf"></a>`unit`: `export`

### Declared structure

- <a id="s-38fbec4d3a"></a>`kind`: `"constant"`
- <a id="s-72ed47de99"></a>`value`: `"^[0-9a-f]{64}$"`

## Governing policies

- <a id="pa-19beca5216"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.SHA256_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3ef4fe053fe1df8a755e0d57a07aa6fc78f29f4fcc4ac61c86fc3cab9e7131e -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^[0-9a-f]{64}$"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "SHA256_PATTERN",
  "unit": "export"
}
```

</details>
