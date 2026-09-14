# stove0_core.Stove0RiverhogClient.project_target_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-project-b7791a888d:31130b7993 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e30b6dc3a9"></a>
- <a id="s-1c26382cca"></a>`distribution`: `stove0-server`
- <a id="s-d3ba5d9127"></a>`module`: `stove0_core`
- <a id="s-22f8735152"></a>`name`: `project_target_dispositions`
- <a id="s-da3a02ca1c"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-c5bf5716d1"></a>`unit`: `member`

### Declared structure

- <a id="s-ec4368444e"></a>`kind`: `"method"`
- <a id="s-7fe43cb670"></a>`signature`: `"\"(self, record: 'WorkRecord', dispositions: 'Sequence[ArtifactDisposition]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-7af03f1820"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.project_target_dispositions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 298ffc8d2d20890d7c54d4f7011b177bc15cb467be7bd1a2b73f130a87f9363d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', dispositions: 'Sequence[ArtifactDisposition]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "project_target_dispositions",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```
