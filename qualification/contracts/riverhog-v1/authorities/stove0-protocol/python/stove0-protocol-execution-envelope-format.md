# stove0_protocol.EXECUTION_ENVELOPE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-execution-envelope-format:10f84c5d77 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e200146562"></a>
- <a id="s-c09bb942ce"></a>`distribution`: `stove0-protocol`
- <a id="s-42138fc648"></a>`module`: `stove0_protocol`
- <a id="s-08f668393e"></a>`name`: `EXECUTION_ENVELOPE_FORMAT`
- <a id="s-b2cfb40704"></a>`unit`: `export`

### Declared structure

- <a id="s-81a4faffd4"></a>`kind`: `"constant"`
- <a id="s-169bd883e2"></a>`value`: `"stove0-execution-envelope/v1"`

## Governing policies

- <a id="pa-ef9a37cf1f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.EXECUTION_ENVELOPE_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b0248c17cb32dd6780dfc45336597ba19cf83c51fe9b4fe841c9a70cea3b299 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-execution-envelope/v1"
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "EXECUTION_ENVELOPE_FORMAT",
  "unit": "export"
}
```

</details>
