# stove0_target_protocol.ExternalEffectReceipt.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-externaleffectreceipt-seal:dfee3dc7b4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dfb3597290"></a>
- <a id="s-7796d7d42f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-7fd88acb7e"></a>`module`: `stove0_target_protocol`
- <a id="s-ceac229e87"></a>`name`: `seal`
- <a id="s-38cdf37ef2"></a>`owner`: `stove0_target_protocol.ExternalEffectReceipt`
- <a id="s-db53130318"></a>`unit`: `member`

### Declared structure

- <a id="s-6becfc66a6"></a>`kind`: `"classmethod"`
- <a id="s-a83b22151c"></a>`signature`: `"\"(cls, payload: 'ExternalEffectReceiptPayload') -> 'ExternalEffectReceipt'\""`

## Maintained corroboration

### Related interface records

- [ExternalEffectReceipt](stove0-target-protocol-externaleffectreceipt.md)

## Governing policies

- <a id="pa-585f45c21f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.ExternalEffectReceipt.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: adcc74fc95d414bcd72fbd33d3ad22eda2a1577f87828437cd8874e8b0d2ac78 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ExternalEffectReceiptPayload') -> 'ExternalEffectReceipt'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.ExternalEffectReceipt",
  "unit": "member"
}
```

</details>
