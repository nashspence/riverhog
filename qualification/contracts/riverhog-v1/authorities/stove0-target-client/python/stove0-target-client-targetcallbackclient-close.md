# stove0_target_client.TargetCallbackClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetcallbackclient-close:9c98862302 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a2f9bfc51"></a>
- <a id="s-7df93868dc"></a>`distribution`: `stove0-target-client`
- <a id="s-cd2382701c"></a>`module`: `stove0_target_client`
- <a id="s-68bfb5ed0d"></a>`name`: `close`
- <a id="s-f1ed62280a"></a>`owner`: `stove0_target_client.TargetCallbackClient`
- <a id="s-bbf01bf978"></a>`unit`: `member`

### Declared structure

- <a id="s-baa8136e09"></a>`kind`: `"method"`
- <a id="s-72e2ad11dd"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [TargetCallbackClient](stove0-target-client-targetcallbackclient.md)

## Governing policies

- <a id="pa-230e59c716"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [some-implementations/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetCallbackClient.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3dc60fdc708afe0c9afb48d7dd397199da9f46a05e09f7cd5d0be8aea6acf880 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "close",
  "owner": "stove0_target_client.TargetCallbackClient",
  "unit": "member"
}
```

</details>
