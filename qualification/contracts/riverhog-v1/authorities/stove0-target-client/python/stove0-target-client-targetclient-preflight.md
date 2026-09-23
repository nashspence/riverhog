# stove0_target_client.TargetClient.preflight

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetclient-preflight:bf5c2370f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-00031d3a12"></a>
- <a id="s-28455bfb59"></a>`distribution`: `stove0-target-client`
- <a id="s-419aa4f8c6"></a>`module`: `stove0_target_client`
- <a id="s-6ce01a03a3"></a>`name`: `preflight`
- <a id="s-25ca1bb3fc"></a>`owner`: `stove0_target_client.TargetClient`
- <a id="s-dab443cc4b"></a>`unit`: `member`

### Declared structure

- <a id="s-15e6f82fc0"></a>`kind`: `"method"`
- <a id="s-16f25a89e0"></a>`signature`: `"\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""`

## Maintained corroboration

### Related interface records

- [TargetClient](stove0-target-client-targetclient.md)

## Governing policies

- <a id="pa-9f710b2b8b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources/authorities.md#src-be4c80156f) — [some-implementations/stove0/packages/target-client/src/stove0\_target\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-client/src/stove0_target_client/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_client.TargetClient.preflight`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c40ac6ce40b6ba208a3432c4db8de62a3d0f0fef961707330ac5ad4fdd7a523 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'TargetPreflightRequest') -> 'TargetPreflightResponse'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "preflight",
  "owner": "stove0_target_client.TargetClient",
  "unit": "member"
}
```

</details>
