# stove0_core.Stove0RiverhogClient.observation_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-observat-3eff7b5fa0:db2856c472 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-18c10d0450"></a>
- <a id="s-e0f15720a5"></a>`distribution`: `stove0-server`
- <a id="s-f861f3713c"></a>`module`: `stove0_core`
- <a id="s-684e1337e3"></a>`name`: `observation_authority`
- <a id="s-17d116359f"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-8edf40f43f"></a>`unit`: `member`

### Declared structure

- <a id="s-798369695b"></a>`kind`: `"method"`
- <a id="s-e319116a54"></a>`signature`: `"\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-11ffb44d7d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.observation_authority`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 27920100f80a66bd7fbb05ef740d0232b3d2e18cbe1a5248c1e895dc650a5668 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', request: 'ObservationRequest') -> 'ObserverRuntimeAuthority'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_authority",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
