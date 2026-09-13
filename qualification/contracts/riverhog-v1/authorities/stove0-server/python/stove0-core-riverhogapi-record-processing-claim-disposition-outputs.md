# stove0_core.RiverhogApi.record_processing_claim_disposition_outputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-record-processing-0489832991:40578a8a72 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7c76c57525"></a>
| Field | Shape |
|---|---|
| <a id="s-7a29a79901"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9d8d9d0e68"></a>`distribution` | "stove0-server" |
| <a id="s-44959407d2"></a>`module` | "stove0_core" |
| <a id="s-0a1a1005c9"></a>`name` | "record_processing_claim_disposition_outputs" |
| <a id="s-59d8c6f310"></a>`owner` | "stove0_core.RiverhogApi" |
| <a id="s-28db2ca614"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_core.RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-00267ab448"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.record_processing_claim_disposition_outputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ad2b3ece3a64f809f902391e2b2c4ff2f107fcc7d1dd0dff2ba7f142dd00cf88 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', outputs: 'Sequence[Mapping[str, Any]]') -> 'ArtifactDispositionSetDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "record_processing_claim_disposition_outputs",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```
