# stove0_core.WorkStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workstore:6d64350e7e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bea4c60051"></a>
- <a id="s-ddb48c006c"></a>`distribution`: `stove0-server`
- <a id="s-817141cb92"></a>`module`: `stove0_core`
- <a id="s-94500012a0"></a>`name`: `WorkStore`
- <a id="s-781d855b3f"></a>`unit`: `export`

### Declared structure

- <a id="s-5017ba5ebe"></a>`kind`: `"class"`
- <a id="s-287a5e0ae4"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [stove0_core.WorkStore.admit_branch_set](stove0-core-workstore-admit-branch-set.md)
- [stove0_core.WorkStore.admit_join](stove0-core-workstore-admit-join.md)
- [stove0_core.WorkStore.compare_and_swap_target_production_seal](stove0-core-workstore-compare-and-swap-target-production-seal.md)
- [stove0_core.WorkStore.compare_and_swap_target_settlement_seal](stove0-core-workstore-compare-and-swap-target-settlement-seal.md)
- [stove0_core.WorkStore.compare_and_swap](stove0-core-workstore-compare-and-swap.md)
- [stove0_core.WorkStore.create](stove0-core-workstore-create.md)
- [stove0_core.WorkStore.ensure_target_production_receiving](stove0-core-workstore-ensure-target-production-receiving.md)
- [stove0_core.WorkStore.ensure_target_settlement_binding](stove0-core-workstore-ensure-target-settlement-binding.md)
- [stove0_core.WorkStore.iter_selection_artifacts](stove0-core-workstore-iter-selection-artifacts.md)
- [stove0_core.WorkStore.iter_target_dispositions](stove0-core-workstore-iter-target-dispositions.md)
- [stove0_core.WorkStore.iter_target_outputs_by_path](stove0-core-workstore-iter-target-outputs-by-path.md)
- [stove0_core.WorkStore.iter_target_outputs](stove0-core-workstore-iter-target-outputs.md)
- [stove0_core.WorkStore.iter_target_source_edges_by_input](stove0-core-workstore-iter-target-source-edges-by-input.md)
- [stove0_core.WorkStore.iter_target_source_edges](stove0-core-workstore-iter-target-source-edges.md)
- [stove0_core.WorkStore.load_selection_artifact](stove0-core-workstore-load-selection-artifact.md)
- [stove0_core.WorkStore.load_selection_ref](stove0-core-workstore-load-selection-ref.md)
- [stove0_core.WorkStore.load_selection](stove0-core-workstore-load-selection.md)
- [stove0_core.WorkStore.load_target_disposition](stove0-core-workstore-load-target-disposition.md)
- [stove0_core.WorkStore.load_target_output](stove0-core-workstore-load-target-output.md)
- [stove0_core.WorkStore.load_target_production_seal](stove0-core-workstore-load-target-production-seal.md)
- [stove0_core.WorkStore.load_target_settlement_seal](stove0-core-workstore-load-target-settlement-seal.md)
- [stove0_core.WorkStore.load](stove0-core-workstore-load.md)
- [stove0_core.WorkStore.record_target_disposition](stove0-core-workstore-record-target-disposition.md)
- [stove0_core.WorkStore.record_target_output](stove0-core-workstore-record-target-output.md)
- [stove0_core.WorkStore.record_target_source_edge](stove0-core-workstore-record-target-source-edge.md)
- [stove0_core.WorkStore.retain_selection](stove0-core-workstore-retain-selection.md)
- [stove0_core.WorkStore.scan_target_production_seals](stove0-core-workstore-scan-target-production-seals.md)
- [stove0_core.WorkStore.selection_artifact_page](stove0-core-workstore-selection-artifact-page.md)
- [stove0_core.WorkStore.target_disposition_page](stove0-core-workstore-target-disposition-page.md)
- [stove0_core.WorkStore.target_output_page](stove0-core-workstore-target-output-page.md)
- [stove0_core.WorkStore.target_output_path_page](stove0-core-workstore-target-output-path-page.md)
- [stove0_core.WorkStore.target_source_edge_page](stove0-core-workstore-target-source-edge-page.md)

## Governing policies

- <a id="pa-ee46621fa6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkStore`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 699d211b5d3a3d631720e1b92bf231a7f8660daf770c200cc9776eb11bcb0c30 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkStore",
  "unit": "export"
}
```
