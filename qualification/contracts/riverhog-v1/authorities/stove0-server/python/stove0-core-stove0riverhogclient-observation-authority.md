# stove0_core.Stove0RiverhogClient.observation_authority

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-observat-3eff7b5fa0:db2856c472 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-e319116a54"></a>`signature`: `"\"(self, claim: 'ClaimBinding', request: 'ContentObservationRequest') -> 'ObserverRuntimeAuthority'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-11ffb44d7d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.observation_authority`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba6d18805289eee33f2b99db243458755571b5db17cfac6ad8963c6afd3d0c40 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim: 'ClaimBinding', request: 'ContentObservationRequest') -> 'ObserverRuntimeAuthority'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_authority",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
