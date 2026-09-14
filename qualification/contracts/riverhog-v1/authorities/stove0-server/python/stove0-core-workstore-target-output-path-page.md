# stove0_core.WorkStore.target_output_path_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-target-output-path-page:014b862a30 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6bbeba874d"></a>
- <a id="s-e92eeca33a"></a>`distribution`: `stove0-server`
- <a id="s-68e368ac8c"></a>`module`: `stove0_core`
- <a id="s-4162c7cf97"></a>`name`: `target_output_path_page`
- <a id="s-b416854e92"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-ceea24dbf1"></a>`unit`: `member`

### Declared structure

- <a id="s-365f35d412"></a>`kind`: `"method"`
- <a id="s-ab35f8b350"></a>`signature`: `"\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str \| None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-cce351568e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.target_output_path_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b7ff01be45af30b84b4ecef9c96d08cf4761d0ea888f36ab6d1265c5f13aa381 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, work_id: 'str', job_id: 'str', *, after_path: 'str | None', limit: 'int') -> 'tuple[OutputArtifact, ...]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "target_output_path_page",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
