# stove0_core.SqlAlchemyStateStore.retain_selection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-retain-selection:0dd206372a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-45f8a907ca"></a>
- <a id="s-3543a3d422"></a>`distribution`: `stove0-server`
- <a id="s-a72f36457d"></a>`module`: `stove0_core`
- <a id="s-1ca962e0f3"></a>`name`: `retain_selection`
- <a id="s-7a38ab170f"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-18ef0768f2"></a>`unit`: `member`

### Declared structure

- <a id="s-6b341fbe1a"></a>`kind`: `"method"`
- <a id="s-11cd6ac7ef"></a>`signature`: `"\"(self, selection: 'ArtifactSelection') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-18840eeebc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.retain_selection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 75e54d315cfbf77e58ce5cbc5ec86387bdcffe35c9142954f112e13afd664b23 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection: 'ArtifactSelection') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retain_selection",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```
