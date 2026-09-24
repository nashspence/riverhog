# stove0_core.RiverhogApi.seal_processing_claim_plan

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-seal-processing-claim-plan:5a270d514e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-570d0f39b9"></a>
- <a id="s-123d1ba726"></a>`distribution`: `stove0-server`
- <a id="s-407e89e3ff"></a>`module`: `stove0_core`
- <a id="s-6169869ba8"></a>`name`: `seal_processing_claim_plan`
- <a id="s-9aa249f4cc"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-8be09261e9"></a>`unit`: `member`

### Declared structure

- <a id="s-e3b359c9af"></a>`kind`: `"method"`
- <a id="s-a4c933340a"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', execution_id: 'str', controller_evidence: 'Mapping[str, Any]', controller_evidence_sha256: 'str', operation_id: 'str', operation_sha256: 'str', input_artifacts: 'Iterable[Mapping[str, Any]]', source_collection_retirement_policy: 'SourceCollectionRetirementPolicy' = 'retain', source_collection_retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-2123d2abeb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.seal_processing_claim_plan`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 648bea99db42de125fc1e0fe77e3020470c3529928eb5059c336baa01f2a99a8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', execution_id: 'str', controller_evidence: 'Mapping[str, Any]', controller_evidence_sha256: 'str', operation_id: 'str', operation_sha256: 'str', input_artifacts: 'Iterable[Mapping[str, Any]]', source_collection_retirement_policy: 'SourceCollectionRetirementPolicy' = 'retain', source_collection_retirement_grace_seconds: 'int' = 0) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_processing_claim_plan",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
