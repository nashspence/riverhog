# stove0_target_protocol.TargetDescriptor.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-targetdescriptor-v-db7ec48ced:8879dc9e2e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-89f36a4d83"></a>
- <a id="s-657093f16b"></a>`distribution`: `stove0-target-protocol`
- <a id="s-e4109bda34"></a>`module`: `stove0_target_protocol`
- <a id="s-c0a1ad4a71"></a>`name`: `verify_digest`
- <a id="s-23cbd17d16"></a>`owner`: `stove0_target_protocol.TargetDescriptor`
- <a id="s-19b839e3d3"></a>`unit`: `member`

### Declared structure

- <a id="s-de181ad61b"></a>`kind`: `"method"`
- <a id="s-96f6c1fd49"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-protocol-targetdescriptor.md)

## Governing policies

- <a id="pa-259ba5a0c8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TargetDescriptor.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ea59b384292da0d1dc83e8ef3901e55431d33ff87852a49b5a5347b2f774c3f9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "verify_digest",
  "owner": "stove0_target_protocol.TargetDescriptor",
  "unit": "member"
}
```

</details>
