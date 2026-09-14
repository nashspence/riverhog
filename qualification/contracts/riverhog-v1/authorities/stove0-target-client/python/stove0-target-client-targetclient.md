# stove0_target_client.TargetClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-client:stove0-target-client-targetclient:6e1d8983a4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42327b6f6f"></a>
- <a id="s-a575d0cd44"></a>`distribution`: `stove0-target-client`
- <a id="s-5f8032e4fe"></a>`module`: `stove0_target_client`
- <a id="s-619c7152e9"></a>`name`: `TargetClient`
- <a id="s-dfc8c10b68"></a>`unit`: `export`

### Declared structure

- <a id="s-e46ad81777"></a>`kind`: `"class"`
- <a id="s-d97514b212"></a>`signature`: `"\"(base_url: 'str', *, token: 'str \| None' = None, timeout: 'float \| None' = 300.0, allow_insecure_http: 'bool' = False) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [cancel](stove0-target-client-targetclient-cancel.md)
- [contract](stove0-target-client-targetclient-contract.md)
- [preflight](stove0-target-client-targetclient-preflight.md)
- [put_job](stove0-target-client-targetclient-put-job.md)
- [status](stove0-target-client-targetclient-status.md)

## Governing policies

- <a id="pa-ae6253e9a2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-client:stove0_target_client](../../../evidence/sources.md#src-be4c80156f) — `reference/stove0/packages/target-client/src/stove0_target_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_client.TargetClient`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3787c85f9446b4b5f0761447cdd6f7619bb2fee81525a91e5b95aa0c6ed9314 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str', *, token: 'str | None' = None, timeout: 'float | None' = 300.0, allow_insecure_http: 'bool' = False) -> 'None'\""
  },
  "distribution": "stove0-target-client",
  "module": "stove0_target_client",
  "name": "TargetClient",
  "unit": "export"
}
```
