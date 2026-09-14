# stove0_protocol.CoordinationSettlement.verify_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationsettlement-ve-dd4f49b0b8:26cc88308e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0888a832de"></a>
- <a id="s-d084251ab3"></a>`distribution`: `stove0-protocol`
- <a id="s-d5a4d9e617"></a>`module`: `stove0_protocol`
- <a id="s-411b8ed84f"></a>`name`: `verify_contract`
- <a id="s-06aad806b5"></a>`owner`: `stove0_protocol.CoordinationSettlement`
- <a id="s-b9222d8724"></a>`unit`: `member`

### Declared structure

- <a id="s-febf5ccb57"></a>`kind`: `"method"`
- <a id="s-3d56e32d7b"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [CoordinationSettlement](stove0-protocol-coordinationsettlement.md)

## Governing policies

- <a id="pa-46b440316b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationSettlement.verify_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 17b0556aacdb3eadaf6b6b82494babf587852d3a9ef9308cfa9eb3312fbf3632 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_contract",
  "owner": "stove0_protocol.CoordinationSettlement",
  "unit": "member"
}
```
