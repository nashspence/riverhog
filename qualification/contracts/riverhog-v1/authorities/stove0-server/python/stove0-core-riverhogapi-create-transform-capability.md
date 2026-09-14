# stove0_core.RiverhogApi.create_transform_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-create-transform-capability:3e92dab62e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf8e0a6a82"></a>
- <a id="s-b24f7a409e"></a>`distribution`: `stove0-server`
- <a id="s-86fe5cbb31"></a>`module`: `stove0_core`
- <a id="s-379d242c90"></a>`name`: `create_transform_capability`
- <a id="s-9ab9b0f75e"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-74ae4389ce"></a>`unit`: `member`

### Declared structure

- <a id="s-d3fff814e3"></a>`kind`: `"method"`
- <a id="s-52c1ef11df"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[Mapping[str, Any]]', ttl_seconds: 'int' = 900) -> 'TransformCapabilityDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-5a02da1f4b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.create_transform_capability`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61775920023115109dfd4a0cceb4e4eb8a640b8deb3f414296a4f6e85fb77221 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[Mapping[str, Any]]', ttl_seconds: 'int' = 900) -> 'TransformCapabilityDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_transform_capability",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
