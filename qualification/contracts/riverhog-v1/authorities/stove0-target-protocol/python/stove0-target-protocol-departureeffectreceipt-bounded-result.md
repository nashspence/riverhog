# stove0_target_protocol.DepartureEffectReceipt.bounded_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-departureeffectrec-e54c3f6397:6672fb71b1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4e1fc5ed12"></a>
- <a id="s-8be6357aa5"></a>`distribution`: `stove0-target-protocol`
- <a id="s-aab0ec05fb"></a>`module`: `stove0_target_protocol`
- <a id="s-536b535ac2"></a>`name`: `bounded_result`
- <a id="s-1ea3f97278"></a>`owner`: `stove0_target_protocol.DepartureEffectReceipt`
- <a id="s-20e88b340c"></a>`unit`: `member`

### Declared structure

- <a id="s-44609a24a3"></a>`kind`: `"method"`
- <a id="s-ce440e6d3e"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectReceipt](stove0-target-protocol-departureeffectreceipt.md)

## Governing policies

- <a id="pa-888c18c872"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.DepartureEffectReceipt.bounded_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7d2c516adb99afc05002875ff80c296710073fad71c7cee7b24181171d4c9fc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "bounded_result",
  "owner": "stove0_target_protocol.DepartureEffectReceipt",
  "unit": "member"
}
```

</details>
