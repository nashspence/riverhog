# stove0_core.SqlAlchemyStateStore.iter_selection_artifacts

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore-iter-sel-be22048004:c662e3024d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6bf5bfed11"></a>
- <a id="s-0b9ba5aa98"></a>`distribution`: `stove0-server`
- <a id="s-b26fc28040"></a>`module`: `stove0_core`
- <a id="s-2370c81f41"></a>`name`: `iter_selection_artifacts`
- <a id="s-d2e47a7455"></a>`owner`: `stove0_core.SqlAlchemyStateStore`
- <a id="s-b180f87f18"></a>`unit`: `member`

### Declared structure

- <a id="s-63164812fa"></a>`kind`: `"method"`
- <a id="s-9214266e0b"></a>`signature`: `"\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""`

## Maintained corroboration

### Related interface records

- [SqlAlchemyStateStore](stove0-core-sqlalchemystatestore.md)

## Governing policies

- <a id="pa-c2cf8fe149"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore.iter_selection_artifacts`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c50b56a420503f813136490dff25231d65ce0d026cfc0b49f01f7b0d1e32ba90 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, selection_sha256: 'str') -> 'Iterator[ArtifactSubject]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "iter_selection_artifacts",
  "owner": "stove0_core.SqlAlchemyStateStore",
  "unit": "member"
}
```

</details>
