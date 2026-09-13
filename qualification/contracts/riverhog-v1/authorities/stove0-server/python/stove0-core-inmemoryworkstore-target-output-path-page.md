# stove0_core.InMemoryWorkStore.target_output_path_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore-target-outp-ddbcef22d8:5301494613 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ba04996ef6"></a>
| Field | Shape |
|---|---|
| <a id="s-83b759d8fc"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-723bb9a1bf"></a>`distribution` | "stove0-server" |
| <a id="s-0b20d3880d"></a>`module` | "stove0_core" |
| <a id="s-91c5107a6e"></a>`name` | "target_output_path_page" |
| <a id="s-b59d5b6321"></a>`owner` | "stove0_core.InMemoryWorkStore" |
| <a id="s-b50aeefdd5"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore](stove0-core-inmemoryworkstore.md)

## Governing policies

- <a id="pa-24b4a706a7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore.target_output_path_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63f8922f6bb370f60624857955bb4aa031c0f0c6a17da11a2adae0c4923c1238 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_output_path_page",
  "owner": "stove0_core.InMemoryWorkStore",
  "unit": "member"
}
```
