# stove0_target_protocol.TargetProductionAuthority.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionau-a47b31d3c0:a549919252 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c26580a7c1"></a>
- <a id="s-c40e147cca"></a>`distribution`: `stove0-target-protocol`
- <a id="s-976ac6de47"></a>`module`: `stove0_target_protocol`
- <a id="s-f5caeaa4a3"></a>`name`: `verify_digest`
- <a id="s-3e25b2387f"></a>`owner`: `stove0_target_protocol.TargetProductionAuthority`
- <a id="s-cb4ea05f5d"></a>`unit`: `member`

### Declared structure

- <a id="s-3f64a70aa1"></a>`kind`: `"method"`
- <a id="s-eb68e6829e"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetProductionAuthority](stove0-target-protocol-targetproductionauthority.md)

## Governing policies

- <a id="pa-612d09ef10"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionAuthority.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fe30bb463284c04baa9e72e70925e9e12a0d7a289b86ad06adfc0a9471fc318a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.TargetProductionAuthority",
  "unit": "member"
}
```

</details>
