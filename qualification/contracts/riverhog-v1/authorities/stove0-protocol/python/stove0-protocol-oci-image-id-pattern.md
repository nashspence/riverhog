# stove0_protocol.OCI_IMAGE_ID_PATTERN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-oci-image-id-pattern:a39031e497 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7289e02ebc"></a>
- <a id="s-5da8487240"></a>`distribution`: `stove0-protocol`
- <a id="s-cb90b3e3bd"></a>`module`: `stove0_protocol`
- <a id="s-6cd928623c"></a>`name`: `OCI_IMAGE_ID_PATTERN`
- <a id="s-804fd78a60"></a>`unit`: `export`

### Declared structure

- <a id="s-cd5406e189"></a>`kind`: `"constant"`
- <a id="s-30393eb141"></a>`value`: `"^sha256:[0-9a-f]{64}$"`

## Governing policies

- <a id="pa-8b8e79c81b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.OCI_IMAGE_ID_PATTERN`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59e3a90e8be2035ba31351a0f39c5bfb1240a4bfedba5fe2f46b361652eb603a -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "^sha256:[0-9a-f]{64}$"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "OCI_IMAGE_ID_PATTERN",
  "unit": "export"
}
```

</details>
