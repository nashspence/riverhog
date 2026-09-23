# stove0_target_protocol.TargetProductionSealResponse.validate_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetproductionse-078acee356:9815e38731 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9b3a70d01b"></a>
- <a id="s-c9b6ee8f20"></a>`distribution`: `stove0-target-protocol`
- <a id="s-92721cda4a"></a>`module`: `stove0_target_protocol`
- <a id="s-000efd1856"></a>`name`: `validate_state`
- <a id="s-6c09837541"></a>`owner`: `stove0_target_protocol.TargetProductionSealResponse`
- <a id="s-7a5e17bd21"></a>`unit`: `member`

### Declared structure

- <a id="s-5e2fdae416"></a>`kind`: `"method"`
- <a id="s-71eca5c58a"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetProductionSealResponse](stove0-target-protocol-targetproductionsealresponse.md)

## Governing policies

- <a id="pa-22f71b822a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetProductionSealResponse.validate_state`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e2fb218435ab84b0beaaae93f7b92b6ac19941980171de73b5e205127969504 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "validate_state",
  "owner": "stove0_target_protocol.TargetProductionSealResponse",
  "unit": "member"
}
```

</details>
