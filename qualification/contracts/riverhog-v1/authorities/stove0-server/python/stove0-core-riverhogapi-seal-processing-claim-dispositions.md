# stove0_core.RiverhogApi.seal_processing_claim_dispositions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-seal-processing-c-85ab9e58e5:45a237a3e8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8ddc1e7871"></a>
- <a id="s-d721140fb8"></a>`distribution`: `stove0-server`
- <a id="s-a45825de46"></a>`module`: `stove0_core`
- <a id="s-a3687bbe9f"></a>`name`: `seal_processing_claim_dispositions`
- <a id="s-e69c000894"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-cc536f710e"></a>`unit`: `member`

### Declared structure

- <a id="s-dd4b812924"></a>`kind`: `"method"`
- <a id="s-dd798ff7dd"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int') -> 'ArtifactDispositionSetDocument'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-d1d7f00404"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.seal_processing_claim_dispositions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8471ce091bacc1a642ae8948077066bc4f399def19916e6c9ca3eb7187eea7d3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "seal_processing_claim_dispositions",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
