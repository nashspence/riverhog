# stove0_core.RiverhogApi

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi:494ee51ffa -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98caf50b83"></a>
- <a id="s-6a147e7e7d"></a>`distribution`: `stove0-server`
- <a id="s-8aeb64117c"></a>`module`: `stove0_core`
- <a id="s-4d27c2ed7b"></a>`name`: `RiverhogApi`
- <a id="s-8866b57f13"></a>`unit`: `export`

### Declared structure

- <a id="s-c8638c0a35"></a>`kind`: `"class"`
- <a id="s-c6c41c5885"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [abandon_processing_claim](stove0-core-riverhogapi-abandon-processing-claim.md)
- [begin_processing_claim_retirement](stove0-core-riverhogapi-begin-processing-claim-retirement.md)
- [create_or_resume_processing_claim](stove0-core-riverhogapi-create-or-resume-processing-claim.md)
- [create_processing_capability](stove0-core-riverhogapi-create-processing-capability.md)
- [delete_collection](stove0-core-riverhogapi-delete-collection.md)
- [get_collection_derivation](stove0-core-riverhogapi-get-collection-derivation.md)
- [get_collection](stove0-core-riverhogapi-get-collection.md)
- [get_portable_collection_inventory](stove0-core-riverhogapi-get-portable-collection-inventory.md)
- [get_processing_claim_dispositions](stove0-core-riverhogapi-get-processing-claim-dispositions.md)
- [get_processing_claim](stove0-core-riverhogapi-get-processing-claim.md)
- [list_processing_claim_dispositions](stove0-core-riverhogapi-list-processing-claim-dispositions.md)
- [list_processing_claim_disposition_outputs](stove0-core-riverhogapi-list-processing-claim-disposition-outputs.md)
- [list_processing_claim_outcomes](stove0-core-riverhogapi-list-processing-claim-outcomes.md)
- [plan_collection_deletion](stove0-core-riverhogapi-plan-collection-deletion.md)
- [record_processing_claim_disposition_outputs](stove0-core-riverhogapi-record-processing-claim-disposition-outputs.md)
- [record_processing_claim_dispositions](stove0-core-riverhogapi-record-processing-claim-dispositions.md)
- [release_processing_claim](stove0-core-riverhogapi-release-processing-claim.md)
- [renew_processing_claim](stove0-core-riverhogapi-renew-processing-claim.md)
- [restart_processing_claim](stove0-core-riverhogapi-restart-processing-claim.md)
- [seal_processing_claim_dispositions](stove0-core-riverhogapi-seal-processing-claim-dispositions.md)
- [seal_processing_claim_plan](stove0-core-riverhogapi-seal-processing-claim-plan.md)
- [settle_processing_claim](stove0-core-riverhogapi-settle-processing-claim.md)
- [settle_processing_claim_outcomes](stove0-core-riverhogapi-settle-processing-claim-outcomes.md)

## Governing policies

- <a id="pa-28a54d6af5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 938d309e3a11352dcb727f2644c1c981a68b9b23220c92688cd24271e8be4b91 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "RiverhogApi",
  "unit": "export"
}
```

</details>
