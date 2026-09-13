# stove0_core.InMemoryWorkStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-inmemoryworkstore:1aba45acb8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c33e4db663"></a>
| Field | Shape |
|---|---|
| <a id="s-6de8a22a5a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-0881768a03"></a>`distribution` | "stove0-server" |
| <a id="s-8ecf933877"></a>`module` | "stove0_core" |
| <a id="s-39e081b462"></a>`name` | "InMemoryWorkStore" |
| <a id="s-4227d138c9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.InMemoryWorkStore.admit_branch_set](stove0-core-inmemoryworkstore-admit-branch-set.md)
- [stove0_core.InMemoryWorkStore.admit_join](stove0-core-inmemoryworkstore-admit-join.md)
- [stove0_core.InMemoryWorkStore.compare_and_swap_target_production_seal](stove0-core-inmemoryworkstore-compare-and-swap-target-production-seal.md)
- [stove0_core.InMemoryWorkStore.compare_and_swap_target_settlement_seal](stove0-core-inmemoryworkstore-compare-and-swap-target-settlement-seal.md)
- [stove0_core.InMemoryWorkStore.compare_and_swap](stove0-core-inmemoryworkstore-compare-and-swap.md)
- [stove0_core.InMemoryWorkStore.create](stove0-core-inmemoryworkstore-create.md)
- [stove0_core.InMemoryWorkStore.ensure_target_production_receiving](stove0-core-inmemoryworkstore-ensure-target-production-receiving.md)
- [stove0_core.InMemoryWorkStore.ensure_target_settlement_binding](stove0-core-inmemoryworkstore-ensure-target-settlement-binding.md)
- [stove0_core.InMemoryWorkStore.iter_selection_artifacts](stove0-core-inmemoryworkstore-iter-selection-artifacts.md)
- [stove0_core.InMemoryWorkStore.iter_target_dispositions](stove0-core-inmemoryworkstore-iter-target-dispositions.md)
- [stove0_core.InMemoryWorkStore.iter_target_source_edges_by_input](stove0-core-inmemoryworkstore-iter-target-source-edges-by-input.md)
- [stove0_core.InMemoryWorkStore.iter_target_outputs_by_path](stove0-core-inmemoryworkstore-iter-target-outputs-by-path.md)
- [stove0_core.InMemoryWorkStore.iter_target_source_edges](stove0-core-inmemoryworkstore-iter-target-source-edges.md)
- [stove0_core.InMemoryWorkStore.iter_target_outputs](stove0-core-inmemoryworkstore-iter-target-outputs.md)
- [stove0_core.InMemoryWorkStore.load_selection_artifact](stove0-core-inmemoryworkstore-load-selection-artifact.md)
- [stove0_core.InMemoryWorkStore.load_selection_ref](stove0-core-inmemoryworkstore-load-selection-ref.md)
- [stove0_core.InMemoryWorkStore.load_selection](stove0-core-inmemoryworkstore-load-selection.md)
- [stove0_core.InMemoryWorkStore.load_target_settlement_seal](stove0-core-inmemoryworkstore-load-target-settlement-seal.md)
- [stove0_core.InMemoryWorkStore.load_target_disposition](stove0-core-inmemoryworkstore-load-target-disposition.md)
- [stove0_core.InMemoryWorkStore.load_target_production_seal](stove0-core-inmemoryworkstore-load-target-production-seal.md)
- [stove0_core.InMemoryWorkStore.load_target_output](stove0-core-inmemoryworkstore-load-target-output.md)
- [stove0_core.InMemoryWorkStore.load](stove0-core-inmemoryworkstore-load.md)
- [stove0_core.InMemoryWorkStore.record_target_disposition](stove0-core-inmemoryworkstore-record-target-disposition.md)
- [stove0_core.InMemoryWorkStore.record_target_source_edge](stove0-core-inmemoryworkstore-record-target-source-edge.md)
- [stove0_core.InMemoryWorkStore.record_target_output](stove0-core-inmemoryworkstore-record-target-output.md)
- [stove0_core.InMemoryWorkStore.retain_selection](stove0-core-inmemoryworkstore-retain-selection.md)
- [stove0_core.InMemoryWorkStore.scan_target_production_seals](stove0-core-inmemoryworkstore-scan-target-production-seals.md)
- [stove0_core.InMemoryWorkStore.selection_artifact_page](stove0-core-inmemoryworkstore-selection-artifact-page.md)
- [stove0_core.InMemoryWorkStore.target_disposition_page](stove0-core-inmemoryworkstore-target-disposition-page.md)
- [stove0_core.InMemoryWorkStore.target_output_path_page](stove0-core-inmemoryworkstore-target-output-path-page.md)
- [stove0_core.InMemoryWorkStore.target_output_page](stove0-core-inmemoryworkstore-target-output-page.md)
- [stove0_core.InMemoryWorkStore.target_source_edge_page](stove0-core-inmemoryworkstore-target-source-edge-page.md)

## Governing policies

- <a id="pa-5a70ae68b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 85da438a4f50ad57d17ca7bc40ca36afb56eff23416a87ebb862811604122e2d -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"() -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "InMemoryWorkStore",
  "unit": "export"
}
```
