# stove0_protocol.BranchSetPlan.verify_contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-branchsetplan-verify-contract:52ce489cea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bcd1e13dd8"></a>
- <a id="s-bec49ea627"></a>`distribution`: `stove0-protocol`
- <a id="s-ae96e06c08"></a>`module`: `stove0_protocol`
- <a id="s-6599c83efd"></a>`name`: `verify_contract`
- <a id="s-7d8c7f72fe"></a>`owner`: `stove0_protocol.BranchSetPlan`
- <a id="s-91e33e4206"></a>`unit`: `member`

### Declared structure

- <a id="s-f093d94ecd"></a>`kind`: `"method"`
- <a id="s-93466cce8d"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [BranchSetPlan](stove0-protocol-branchsetplan.md)

## Governing policies

- <a id="pa-0425b6e119"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [reference/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.BranchSetPlan.verify_contract`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19e55c96414d1612b3f0814bcd5a8403779531113a146ce7d5234b9f58b9de0a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "verify_contract",
  "owner": "stove0_protocol.BranchSetPlan",
  "unit": "member"
}
```

</details>
