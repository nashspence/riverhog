# stove0_target_protocol.AcceptedTargetJob.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-acceptedtargetjob-d5ced91e34:59d9d2d18e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e674a4e31"></a>
- <a id="s-576afcbc69"></a>`distribution`: `stove0-target-protocol`
- <a id="s-f59b9475ea"></a>`module`: `stove0_target_protocol`
- <a id="s-ad7bc4d508"></a>`name`: `verify_digest`
- <a id="s-19177580e8"></a>`owner`: `stove0_target_protocol.AcceptedTargetJob`
- <a id="s-a1a1c93d1d"></a>`unit`: `member`

### Declared structure

- <a id="s-34f6c31e7a"></a>`kind`: `"method"`
- <a id="s-1af9d078d7"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [AcceptedTargetJob](stove0-target-protocol-acceptedtargetjob.md)

## Governing policies

- <a id="pa-82ba47e5ec"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.AcceptedTargetJob.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b8576053cb418b704fd6db31d484b04af27d2d673365ec9bb3fb09baa803868 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.AcceptedTargetJob",
  "unit": "member"
}
```

</details>
