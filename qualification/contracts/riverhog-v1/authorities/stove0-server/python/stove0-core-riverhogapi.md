# stove0_core.RiverhogApi

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi:494ee51ffa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98caf50b83"></a>
| Field | Shape |
|---|---|
| <a id="s-d7c45a3137"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-6a147e7e7d"></a>`distribution` | "stove0-server" |
| <a id="s-8aeb64117c"></a>`module` | "stove0_core" |
| <a id="s-4d27c2ed7b"></a>`name` | "RiverhogApi" |
| <a id="s-8866b57f13"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi.abandon_processing_claim](stove0-core-riverhogapi-abandon-processing-claim.md)
- [stove0_core.RiverhogApi.begin_processing_claim_retirement](stove0-core-riverhogapi-begin-processing-claim-retirement.md)
- [stove0_core.RiverhogApi.create_or_resume_processing_claim](stove0-core-riverhogapi-create-or-resume-processing-claim.md)
- [stove0_core.RiverhogApi.create_transform_capability](stove0-core-riverhogapi-create-transform-capability.md)
- [stove0_core.RiverhogApi.delete_collection](stove0-core-riverhogapi-delete-collection.md)
- [stove0_core.RiverhogApi.get_collection_derivation](stove0-core-riverhogapi-get-collection-derivation.md)
- [stove0_core.RiverhogApi.get_collection](stove0-core-riverhogapi-get-collection.md)
- [stove0_core.RiverhogApi.get_portable_collection_inventory](stove0-core-riverhogapi-get-portable-collection-inventory.md)
- [stove0_core.RiverhogApi.get_processing_claim_dispositions](stove0-core-riverhogapi-get-processing-claim-dispositions.md)
- [stove0_core.RiverhogApi.get_processing_claim](stove0-core-riverhogapi-get-processing-claim.md)
- [stove0_core.RiverhogApi.list_processing_claim_dispositions](stove0-core-riverhogapi-list-processing-claim-dispositions.md)
- [stove0_core.RiverhogApi.list_processing_claim_disposition_outputs](stove0-core-riverhogapi-list-processing-claim-disposition-outputs.md)
- [stove0_core.RiverhogApi.list_processing_claim_outcomes](stove0-core-riverhogapi-list-processing-claim-outcomes.md)
- [stove0_core.RiverhogApi.plan_collection_deletion](stove0-core-riverhogapi-plan-collection-deletion.md)
- [stove0_core.RiverhogApi.record_processing_claim_disposition_outputs](stove0-core-riverhogapi-record-processing-claim-disposition-outputs.md)
- [stove0_core.RiverhogApi.record_processing_claim_dispositions](stove0-core-riverhogapi-record-processing-claim-dispositions.md)
- [stove0_core.RiverhogApi.release_processing_claim](stove0-core-riverhogapi-release-processing-claim.md)
- [stove0_core.RiverhogApi.renew_processing_claim](stove0-core-riverhogapi-renew-processing-claim.md)
- [stove0_core.RiverhogApi.restart_processing_claim](stove0-core-riverhogapi-restart-processing-claim.md)
- [stove0_core.RiverhogApi.seal_processing_claim_dispositions](stove0-core-riverhogapi-seal-processing-claim-dispositions.md)
- [stove0_core.RiverhogApi.seal_processing_claim_plan](stove0-core-riverhogapi-seal-processing-claim-plan.md)
- [stove0_core.RiverhogApi.settle_processing_claim](stove0-core-riverhogapi-settle-processing-claim.md)
- [stove0_core.RiverhogApi.settle_processing_claim_outcomes](stove0-core-riverhogapi-settle-processing-claim-outcomes.md)

## Governing policies

- <a id="pa-28a54d6af5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi`

### Exact owned JSON

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
