# stove0_target_client.TargetClient.contract

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetclient-contract:e86e91dc96 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8147080d3f"></a>
- <a id="s-547e3f1fae"></a>`distribution`: `stove0-target-client`
- <a id="s-91f70d2a3c"></a>`module`: `stove0_target_client`
- <a id="s-f58acbe94f"></a>`name`: `contract`
- <a id="s-3f59f4ec47"></a>`owner`: `stove0_target_client.TargetClient`
- <a id="s-cf94601d92"></a>`unit`: `member`

### Declared structure

- <a id="s-db3baaea09"></a>`kind`: `"method"`
- <a id="s-119c656070"></a>`signature`: `"\"(self) -> 'TargetContract'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_client.TargetClient](stove0-target-client-targetclient.md)

## Governing policies

- <a id="pa-68a5603ed4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_client.TargetClient.contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 168883b577e3e66540d3bf832e37217b8de3124f2aaaff361728f7c58c17fa21 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetContract'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "contract",
  "owner": "stove0_target_client.TargetClient",
  "unit": "member"
}
```
