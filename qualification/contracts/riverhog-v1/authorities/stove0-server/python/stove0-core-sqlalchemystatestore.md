# stove0_core.SqlAlchemyStateStore

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-sqlalchemystatestore:0bbf7e527a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3880bd2f03"></a>
- <a id="s-07fffd3ea4"></a>`distribution`: `stove0-server`
- <a id="s-3f154d00f4"></a>`module`: `stove0_core`
- <a id="s-7877d5e40c"></a>`name`: `SqlAlchemyStateStore`
- <a id="s-59671e259e"></a>`unit`: `export`

### Declared structure

- <a id="s-719ccd2292"></a>`kind`: `"class"`
- <a id="s-b9aad5d17c"></a>`signature`: `"\"(database_url: 'str', *, engine: 'Engine \| None' = None, initialize: 'bool' = True) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [admit_branch_set](stove0-core-sqlalchemystatestore-admit-branch-set.md)
- [admit_join](stove0-core-sqlalchemystatestore-admit-join.md)
- [compare_and_swap_evaluation](stove0-core-sqlalchemystatestore-compare-and-swap-evaluation.md)
- [compare_and_swap_target_production_seal](stove0-core-sqlalchemystatestore-compare-and-swap-target-production-seal.md)
- [compare_and_swap](stove0-core-sqlalchemystatestore-compare-and-swap.md)
- [compare_and_swap_cursor](stove0-core-sqlalchemystatestore-compare-and-swap-cursor.md)
- [compare_and_swap_target_settlement_seal](stove0-core-sqlalchemystatestore-compare-and-swap-target-settlement-seal.md)
- [create_evaluation](stove0-core-sqlalchemystatestore-create-evaluation.md)
- [create](stove0-core-sqlalchemystatestore-create.md)
- [ensure_target_production_receiving](stove0-core-sqlalchemystatestore-ensure-target-production-receiving.md)
- [ensure_target_settlement_binding](stove0-core-sqlalchemystatestore-ensure-target-settlement-binding.md)
- [evaluation_store](stove0-core-sqlalchemystatestore-evaluation-store.md)
- [iter_evaluations](stove0-core-sqlalchemystatestore-iter-evaluations.md)
- [iter_selection_artifacts](stove0-core-sqlalchemystatestore-iter-selection-artifacts.md)
- [iter_target_outputs_by_path](stove0-core-sqlalchemystatestore-iter-target-outputs-by-path.md)
- [iter_target_source_edges_by_input](stove0-core-sqlalchemystatestore-iter-target-source-edges-by-input.md)
- [iter_target_dispositions](stove0-core-sqlalchemystatestore-iter-target-dispositions.md)
- [iter_target_source_edges](stove0-core-sqlalchemystatestore-iter-target-source-edges.md)
- [iter_target_outputs](stove0-core-sqlalchemystatestore-iter-target-outputs.md)
- [iter_work](stove0-core-sqlalchemystatestore-iter-work.md)
- [list_evaluations](stove0-core-sqlalchemystatestore-list-evaluations.md)
- [list_events](stove0-core-sqlalchemystatestore-list-events.md)
- [list_work](stove0-core-sqlalchemystatestore-list-work.md)
- [load_cursor](stove0-core-sqlalchemystatestore-load-cursor.md)
- [load_evaluation](stove0-core-sqlalchemystatestore-load-evaluation.md)
- [load_selection_artifact](stove0-core-sqlalchemystatestore-load-selection-artifact.md)
- [load_selection_ref](stove0-core-sqlalchemystatestore-load-selection-ref.md)
- [load_selection](stove0-core-sqlalchemystatestore-load-selection.md)
- [load_target_settlement_seal](stove0-core-sqlalchemystatestore-load-target-settlement-seal.md)
- [load_target_disposition](stove0-core-sqlalchemystatestore-load-target-disposition.md)
- [load_target_production_seal](stove0-core-sqlalchemystatestore-load-target-production-seal.md)
- [load_target_output](stove0-core-sqlalchemystatestore-load-target-output.md)
- [load](stove0-core-sqlalchemystatestore-load.md)
- [prune_operational_state](stove0-core-sqlalchemystatestore-prune-operational-state.md)
- [record_target_disposition](stove0-core-sqlalchemystatestore-record-target-disposition.md)
- [record_target_output](stove0-core-sqlalchemystatestore-record-target-output.md)
- [record_target_source_edge](stove0-core-sqlalchemystatestore-record-target-source-edge.md)
- [retain_selection](stove0-core-sqlalchemystatestore-retain-selection.md)
- [scan_target_production_seals](stove0-core-sqlalchemystatestore-scan-target-production-seals.md)
- [scan_work](stove0-core-sqlalchemystatestore-scan-work.md)
- [selection_artifact_page](stove0-core-sqlalchemystatestore-selection-artifact-page.md)
- [target_disposition_page](stove0-core-sqlalchemystatestore-target-disposition-page.md)
- [target_output_path_page](stove0-core-sqlalchemystatestore-target-output-path-page.md)
- [target_output_page](stove0-core-sqlalchemystatestore-target-output-page.md)
- [target_source_edge_page](stove0-core-sqlalchemystatestore-target-source-edge-page.md)

## Governing policies

- <a id="pa-84bdfe843d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.SqlAlchemyStateStore`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cb565fac4c0d9ee9d1c3d75c3b6d1ac00bb007d59b0158b8d687bfc17f1233f0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(database_url: 'str', *, engine: 'Engine | None' = None, initialize: 'bool' = True) -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "SqlAlchemyStateStore",
  "unit": "export"
}
```

</details>
