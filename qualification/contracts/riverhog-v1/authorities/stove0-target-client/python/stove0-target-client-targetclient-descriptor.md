# stove0_target_client.TargetClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetclient-descriptor:f5a8bfd413 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1a2d1a1b43"></a>
- <a id="s-ca5f74370f"></a>`distribution`: `stove0-target-client`
- <a id="s-e6da680788"></a>`module`: `stove0_target_client`
- <a id="s-7dc036bddb"></a>`name`: `descriptor`
- <a id="s-8a7f5b3802"></a>`owner`: `stove0_target_client.TargetClient`
- <a id="s-b62fd1a3fa"></a>`unit`: `member`

### Declared structure

- <a id="s-e8f1a7991d"></a>`kind`: `"method"`
- <a id="s-2e3a81035e"></a>`signature`: `"\"(self) -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [TargetClient](stove0-target-client-targetclient.md)

## Governing policies

- <a id="pa-9a2be28d7f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [some-implementations/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetClient.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e7ed71d653cfc0fadc2e18fbf1d5ad8a4d72c384daa77def5fcff807bb0729eb -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "descriptor",
  "owner": "stove0_target_client.TargetClient",
  "unit": "member"
}
```

</details>
