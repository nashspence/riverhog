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
- <a id="s-0881768a03"></a>`distribution`: `stove0-server`
- <a id="s-8ecf933877"></a>`module`: `stove0_core`
- <a id="s-39e081b462"></a>`name`: `InMemoryWorkStore`
- <a id="s-4227d138c9"></a>`unit`: `export`

### Declared structure

- <a id="s-015a242cd4"></a>`kind`: `"class"`
- <a id="s-2e62b45da8"></a>`signature`: `"\"() -> 'None'\""`

## Maintained corroboration

### Related interface records

- [admit_branch_set](stove0-core-inmemoryworkstore-admit-branch-set.md)
- [admit_join](stove0-core-inmemoryworkstore-admit-join.md)
- [compare_and_swap_target_production_seal](stove0-core-inmemoryworkstore-compare-and-swap-target-production-seal.md)
- [compare_and_swap_target_settlement_seal](stove0-core-inmemoryworkstore-compare-and-swap-target-settlement-seal.md)
- [compare_and_swap](stove0-core-inmemoryworkstore-compare-and-swap.md)
- [create](stove0-core-inmemoryworkstore-create.md)
- [ensure_target_production_receiving](stove0-core-inmemoryworkstore-ensure-target-production-receiving.md)
- [ensure_target_settlement_binding](stove0-core-inmemoryworkstore-ensure-target-settlement-binding.md)
- [iter_selection_artifacts](stove0-core-inmemoryworkstore-iter-selection-artifacts.md)
- [iter_target_dispositions](stove0-core-inmemoryworkstore-iter-target-dispositions.md)
- [iter_target_source_edges_by_input](stove0-core-inmemoryworkstore-iter-target-source-edges-by-input.md)
- [iter_target_outputs_by_path](stove0-core-inmemoryworkstore-iter-target-outputs-by-path.md)
- [iter_target_source_edges](stove0-core-inmemoryworkstore-iter-target-source-edges.md)
- [iter_target_outputs](stove0-core-inmemoryworkstore-iter-target-outputs.md)
- [load_selection_artifact](stove0-core-inmemoryworkstore-load-selection-artifact.md)
- [load_selection_ref](stove0-core-inmemoryworkstore-load-selection-ref.md)
- [load_selection](stove0-core-inmemoryworkstore-load-selection.md)
- [load_target_settlement_seal](stove0-core-inmemoryworkstore-load-target-settlement-seal.md)
- [load_target_disposition](stove0-core-inmemoryworkstore-load-target-disposition.md)
- [load_target_production_seal](stove0-core-inmemoryworkstore-load-target-production-seal.md)
- [load_target_output](stove0-core-inmemoryworkstore-load-target-output.md)
- [load](stove0-core-inmemoryworkstore-load.md)
- [record_target_disposition](stove0-core-inmemoryworkstore-record-target-disposition.md)
- [record_target_source_edge](stove0-core-inmemoryworkstore-record-target-source-edge.md)
- [record_target_output](stove0-core-inmemoryworkstore-record-target-output.md)
- [retain_selection](stove0-core-inmemoryworkstore-retain-selection.md)
- [scan_target_production_seals](stove0-core-inmemoryworkstore-scan-target-production-seals.md)
- [selection_artifact_page](stove0-core-inmemoryworkstore-selection-artifact-page.md)
- [target_disposition_page](stove0-core-inmemoryworkstore-target-disposition-page.md)
- [target_output_path_page](stove0-core-inmemoryworkstore-target-output-path-page.md)
- [target_output_page](stove0-core-inmemoryworkstore-target-output-page.md)
- [target_source_edge_page](stove0-core-inmemoryworkstore-target-source-edge-page.md)

## Governing policies

- <a id="pa-5a70ae68b5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.InMemoryWorkStore`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
