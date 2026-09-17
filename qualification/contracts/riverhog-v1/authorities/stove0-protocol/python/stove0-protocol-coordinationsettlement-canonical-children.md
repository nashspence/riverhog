# stove0_protocol.CoordinationSettlement.canonical_children

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-coordinationsettlement-ca-ec0606728f:942cd76372 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-05b2fc5bd4"></a>
- <a id="s-5d17808642"></a>`distribution`: `stove0-protocol`
- <a id="s-214fbddf34"></a>`module`: `stove0_protocol`
- <a id="s-0abdcce096"></a>`name`: `canonical_children`
- <a id="s-68adca8a0f"></a>`owner`: `stove0_protocol.CoordinationSettlement`
- <a id="s-0dcf36014b"></a>`unit`: `member`

### Declared structure

- <a id="s-9daa459ee4"></a>`kind`: `"classmethod"`
- <a id="s-7e1c6e4faf"></a>`signature`: `"\"(cls, value: 'tuple[CoordinationChildSettlementRef, ...]') -> 'tuple[CoordinationChildSettlementRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [CoordinationSettlement](stove0-protocol-coordinationsettlement.md)

## Governing policies

- <a id="pa-9017c3fae8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.CoordinationSettlement.canonical_children`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ba3dcf8758ad9f9c7e9c6aa1e0aadd83f45b3f40790ac9fc406e69b0f998123 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[CoordinationChildSettlementRef, ...]') -> 'tuple[CoordinationChildSettlementRef, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_children",
  "owner": "stove0_protocol.CoordinationSettlement",
  "unit": "member"
}
```

</details>
