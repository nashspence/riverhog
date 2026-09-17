# stove0_target_protocol.TargetJobStatus.canonical_derivation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetjobstatus-ca-04d45c5edf:ef4e9aceca -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b0d8db675"></a>
- <a id="s-17ea354f9f"></a>`distribution`: `stove0-target-protocol`
- <a id="s-852710b1c0"></a>`module`: `stove0_target_protocol`
- <a id="s-61bd74f800"></a>`name`: `canonical_derivation`
- <a id="s-3d677da231"></a>`owner`: `stove0_target_protocol.TargetJobStatus`
- <a id="s-f4d6fe4297"></a>`unit`: `member`

### Declared structure

- <a id="s-85532bb5ae"></a>`kind`: `"classmethod"`
- <a id="s-ef3cf4fe1b"></a>`signature`: `"\"(cls, value: 'dict[str, Any] \| None') -> 'dict[str, Any] \| None'\""`

## Maintained corroboration

### Related interface records

- [TargetJobStatus](stove0-target-protocol-targetjobstatus.md)

## Governing policies

- <a id="pa-4c7e514651"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetJobStatus.canonical_derivation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 195d9ad04dc50139c2278642c479a8f5de250d1abaf0128c1f1791d8c972f06e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, Any] | None') -> 'dict[str, Any] | None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "canonical_derivation",
  "owner": "stove0_target_protocol.TargetJobStatus",
  "unit": "member"
}
```

</details>
