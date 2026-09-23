# stove0_core.RecipePlanner.observation_requests

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-recipeplanner-observation-requests:96e3019122 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-a18de5e3ff"></a>`signature`: `"\"(self, work: 'WorkIdentity') -> 'tuple[ContentObservationRequest, ...]'\""`

## Maintained corroboration

### Related interface records

- [RecipePlanner](stove0-core-recipeplanner.md)

## Governing policies

- <a id="pa-67fc327e88"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RecipePlanner.observation_requests`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c6a22ed8ddff8da1657ba2479095452dfe5f9c4630a87998fb8c1bac6122b1aa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work: 'WorkIdentity') -> 'tuple[ContentObservationRequest, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "observation_requests",
  "owner": "stove0_core.RecipePlanner",
  "unit": "member"
}
```

</details>
