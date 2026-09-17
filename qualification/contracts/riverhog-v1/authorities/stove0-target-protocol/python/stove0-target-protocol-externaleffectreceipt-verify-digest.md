# stove0_target_protocol.ExternalEffectReceipt.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-externaleffectrece-7520777aa0:345460e025 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8fac1bae11"></a>
- <a id="s-2e2b407f05"></a>`distribution`: `stove0-target-protocol`
- <a id="s-b27080c410"></a>`module`: `stove0_target_protocol`
- <a id="s-fddbdfd72e"></a>`name`: `verify_digest`
- <a id="s-fe1633cf9d"></a>`owner`: `stove0_target_protocol.ExternalEffectReceipt`
- <a id="s-82fc3b4190"></a>`unit`: `member`

### Declared structure

- <a id="s-238963c943"></a>`kind`: `"method"`
- <a id="s-f9fe9cdabb"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ExternalEffectReceipt](stove0-target-protocol-externaleffectreceipt.md)

## Governing policies

- <a id="pa-79ee7185bc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.ExternalEffectReceipt.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e85154b6cab441efea7a1b87213ff10567ab10422dab6173f4a2e7a68f51230e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.ExternalEffectReceipt",
  "unit": "member"
}
```

</details>
