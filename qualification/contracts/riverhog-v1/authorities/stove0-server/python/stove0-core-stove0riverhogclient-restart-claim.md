# stove0_core.Stove0RiverhogClient.restart_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-restart-claim:e6a3cff01a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ca7bec5e38"></a>
- <a id="s-ccc590ae5e"></a>`distribution`: `stove0-server`
- <a id="s-6582e2db28"></a>`module`: `stove0_core`
- <a id="s-aec20ca02d"></a>`name`: `restart_claim`
- <a id="s-4d80f2cf55"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-386ab4210a"></a>`unit`: `member`

### Declared structure

- <a id="s-114a0542fb"></a>`kind`: `"method"`
- <a id="s-58ad0f1d6d"></a>`signature`: `"\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-aa567551fc"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.restart_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52c647cf9e192ab2ed2d8350bdd99b442cd37689ac190440ca8e8dbaaae3b2da -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity', claim: 'ClaimBinding') -> 'ClaimBinding'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "restart_claim",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
