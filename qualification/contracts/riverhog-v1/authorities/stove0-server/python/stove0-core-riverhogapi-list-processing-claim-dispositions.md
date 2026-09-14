# stove0_core.RiverhogApi.list_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-list-processing-c-028b8191c0:ac471c4616 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41e2d9b63d"></a>
- <a id="s-ff9e08b332"></a>`distribution`: `stove0-server`
- <a id="s-456b5ce972"></a>`module`: `stove0_core`
- <a id="s-829e01bf33"></a>`name`: `list_processing_claim_dispositions`
- <a id="s-72caef6473"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-fa9ba425c7"></a>`unit`: `member`

### Declared structure

- <a id="s-6b0d2bc42d"></a>`kind`: `"method"`
- <a id="s-f1b0fe7995"></a>`signature`: `"\"(self, claim_id: 'str', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionPageDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-5041b60d18"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.list_processing_claim_dispositions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c29202b619e57d1b1390219815adf2d510c9f3ddacb1f8d4912854e2335ff849 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionPageDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_processing_claim_dispositions",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
