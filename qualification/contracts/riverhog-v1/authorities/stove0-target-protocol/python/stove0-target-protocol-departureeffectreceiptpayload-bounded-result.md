# stove0_target_protocol.DepartureEffectReceiptPayload.bounded_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectrec-49f81d59ed:eb1af14f36 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1add1d62be"></a>
- <a id="s-f319274bee"></a>`distribution`: `stove0-target-protocol`
- <a id="s-d9f8bd4d31"></a>`module`: `stove0_target_protocol`
- <a id="s-4bdff4c0e9"></a>`name`: `bounded_result`
- <a id="s-7fe848bc90"></a>`owner`: `stove0_target_protocol.DepartureEffectReceiptPayload`
- <a id="s-dddf8a7d9f"></a>`unit`: `member`

### Declared structure

- <a id="s-b4cd5d75d3"></a>`kind`: `"method"`
- <a id="s-49d4134770"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectReceiptPayload](stove0-target-protocol-departureeffectreceiptpayload.md)

## Governing policies

- <a id="pa-4cfea11b5a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectReceiptPayload.bounded_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 796075c78c71fc01f6fbd62ddaa6d1c5d01c8ed62feb9b51cc10dc433f008df5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bounded_result",
  "owner": "stove0_target_protocol.DepartureEffectReceiptPayload",
  "unit": "member"
}
```

</details>
