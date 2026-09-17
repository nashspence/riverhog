# stove0_core.RiverhogApi.settle_processing_claim

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-settle-processing-claim:490a9d1bba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4dd2d3137d"></a>
- <a id="s-153beb4e14"></a>`distribution`: `stove0-server`
- <a id="s-b47b619a9c"></a>`module`: `stove0_core`
- <a id="s-20a0eed63b"></a>`name`: `settle_processing_claim`
- <a id="s-5b077e7478"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-61eae5ed9e"></a>`unit`: `member`

### Declared structure

- <a id="s-81e84e7840"></a>`kind`: `"method"`
- <a id="s-12ac7d92b5"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', output_collection_id: 'int', derivation: 'Mapping[str, Any]', outcome_claim_id: 'str \| None' = None, outcome_fence: 'int \| None' = None, outcome_id: 'str \| None' = None) -> 'ProcessingClaimDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-7cfd4580e1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.settle_processing_claim`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 679016b135f7dde7f811db00ee3a9f2f35dd1cbba9e82a59e08d5e41587da10f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', output_collection_id: 'int', derivation: 'Mapping[str, Any]', outcome_claim_id: 'str | None' = None, outcome_fence: 'int | None' = None, outcome_id: 'str | None' = None) -> 'ProcessingClaimDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "settle_processing_claim",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
