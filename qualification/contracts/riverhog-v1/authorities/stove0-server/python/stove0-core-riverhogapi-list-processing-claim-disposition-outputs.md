# stove0_core.RiverhogApi.list_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-list-processing-c-0c4211e1bf:0c33e11fa3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-048af0937d"></a>
- <a id="s-31fb1cb97a"></a>`distribution`: `stove0-server`
- <a id="s-f2ed7122d0"></a>`module`: `stove0_core`
- <a id="s-ad1e59e454"></a>`name`: `list_processing_claim_disposition_outputs`
- <a id="s-6bbf455efa"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-28e2834a5b"></a>`unit`: `member`

### Declared structure

- <a id="s-e18577fb44"></a>`kind`: `"method"`
- <a id="s-6f74bc2311"></a>`signature`: `"\"(self, claim_id: 'str', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionOutputPageDocument'\""`

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-117fbc59c8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.list_processing_claim_disposition_outputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8250f750d72cec0ec16d3cccbab4dfcab3c562bcf38da93c6c46f4fbe883684b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, authority_sha256: 'str', start_ordinal: 'int' = 0) -> 'ArtifactDispositionOutputPageDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "list_processing_claim_disposition_outputs",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
