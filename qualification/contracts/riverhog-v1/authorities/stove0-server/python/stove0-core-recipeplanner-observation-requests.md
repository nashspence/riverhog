# stove0_core.RecipePlanner.observation_requests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-observation-requests:96e3019122 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d10664b99"></a>
- <a id="s-0904d866d6"></a>`distribution`: `stove0-server`
- <a id="s-b9b8099f16"></a>`module`: `stove0_core`
- <a id="s-7561ea4903"></a>`name`: `observation_requests`
- <a id="s-65bd5defbe"></a>`owner`: `stove0_core.RecipePlanner`
- <a id="s-cc974b8462"></a>`unit`: `member`

### Declared structure

- <a id="s-fab8b79b34"></a>`kind`: `"method"`
- <a id="s-a18de5e3ff"></a>`signature`: `"\"(self, work: 'WorkIdentity') -> 'tuple[ObservationRequest, ...]'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-67fc327e88"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.observation_requests`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52bd41a8cf94c2190b5741e25ab25f513505b5a21187ae30ed6d2a98385810c2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity') -> 'tuple[ObservationRequest, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_requests",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```
