# stove0_core.WorkStore.selection_artifact_page

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore-selection-artifact-page:c55f40b4f5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e159083615"></a>
- <a id="s-07cffb5ae2"></a>`distribution`: `stove0-server`
- <a id="s-3966309693"></a>`module`: `stove0_core`
- <a id="s-043c3a3be1"></a>`name`: `selection_artifact_page`
- <a id="s-be750672e2"></a>`owner`: `stove0_core.WorkStore`
- <a id="s-c0eb1876e8"></a>`unit`: `member`

### Declared structure

- <a id="s-8b3636dd69"></a>`kind`: `"method"`
- <a id="s-1242369a9b"></a>`signature`: `"\"(self, selection_sha256: 'str', *, continuation: 'str \| None', limit: 'int') -> 'tuple[tuple[ArtifactSubject, ...], str \| None, bool]'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore](stove0-core-workstore.md)

## Governing policies

- <a id="pa-c427175611"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore.selection_artifact_page`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cabf6524a0b141e1ef600bd19510bf6209b468df318a59f9b2d2b5fba35feb71 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str', *, continuation: 'str | None', limit: 'int') -> 'tuple[tuple[ArtifactSubject, ...], str | None, bool]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "selection_artifact_page",
  "owner": "stove0_core.WorkStore",
  "unit": "member"
}
```
