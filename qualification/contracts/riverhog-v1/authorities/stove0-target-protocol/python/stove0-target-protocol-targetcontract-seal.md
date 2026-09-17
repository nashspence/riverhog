# stove0_target_protocol.TargetContract.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetcontract-seal:8f046e3ec0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b01589cc6"></a>
- <a id="s-2ff8bedc0c"></a>`distribution`: `stove0-target-protocol`
- <a id="s-7eca700251"></a>`module`: `stove0_target_protocol`
- <a id="s-092d8a1e07"></a>`name`: `seal`
- <a id="s-57635dfc6c"></a>`owner`: `stove0_target_protocol.TargetContract`
- <a id="s-a0922bff43"></a>`unit`: `member`

### Declared structure

- <a id="s-9f7ff1cd9e"></a>`kind`: `"classmethod"`
- <a id="s-6b0aafd046"></a>`signature`: `"\"(cls, payload: 'TargetContractPayload') -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [TargetContract](stove0-target-protocol-targetcontract.md)

## Governing policies

- <a id="pa-02c44a4372"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetContract.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b13a62bb5701c342d7c2660d398783c02da524d6d8f148716aaa90f361d1b485 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TargetContractPayload') -> 'TargetContract'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.TargetContract",
  "unit": "member"
}
```

</details>
