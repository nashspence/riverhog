# stove0_target_protocol.TargetSettlementAuthority.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetsettlementau-237b098db1:a2d2aeb52e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb64a5680d"></a>
- <a id="s-1ca4347e94"></a>`distribution`: `stove0-target-protocol`
- <a id="s-6506ae0575"></a>`module`: `stove0_target_protocol`
- <a id="s-458321a52f"></a>`name`: `seal`
- <a id="s-58c7fcf419"></a>`owner`: `stove0_target_protocol.TargetSettlementAuthority`
- <a id="s-dbd54c8cd4"></a>`unit`: `member`

### Declared structure

- <a id="s-768ed35d32"></a>`kind`: `"classmethod"`
- <a id="s-c8defd7e9f"></a>`signature`: `"\"(cls, payload: 'TargetSettlementAuthorityPayload') -> 'TargetSettlementAuthority'\""`

## Maintained corroboration

### Related interface records

- [TargetSettlementAuthority](stove0-target-protocol-targetsettlementauthority.md)

## Governing policies

- <a id="pa-1111e9e524"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetSettlementAuthority.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d4e55ceb45417424cb5e92bccc9b2a7214549b4c409f6d19064d500afdc9f2b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TargetSettlementAuthorityPayload') -> 'TargetSettlementAuthority'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "seal",
  "owner": "stove0_target_protocol.TargetSettlementAuthority",
  "unit": "member"
}
```

</details>
