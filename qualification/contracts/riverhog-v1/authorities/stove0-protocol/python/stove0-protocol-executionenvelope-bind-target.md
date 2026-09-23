# stove0_protocol.ExecutionEnvelope.bind_target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelope-bind-target:2c402d1597 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f66b58ed01"></a>
- <a id="s-87c10992e0"></a>`distribution`: `stove0-protocol`
- <a id="s-fd52465782"></a>`module`: `stove0_protocol`
- <a id="s-32d3c86c3c"></a>`name`: `bind_target`
- <a id="s-368d1fc836"></a>`owner`: `stove0_protocol.ExecutionEnvelope`
- <a id="s-75772d9a39"></a>`unit`: `member`

### Declared structure

- <a id="s-c56412fd05"></a>`kind`: `"method"`
- <a id="s-e69ffd7fdb"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ExecutionEnvelope](stove0-protocol-executionenvelope.md)

## Governing policies

- <a id="pa-7039a213ee"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelope.bind_target`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 070643da3d01a63cc2e895685fa8f2ab228d46b19f73b39a6b89e23492fede97 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "bind_target",
  "owner": "stove0_protocol.ExecutionEnvelope",
  "unit": "member"
}
```

</details>
