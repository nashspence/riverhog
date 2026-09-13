# riverhog_protocol.ProcessingClaimAbandonDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimabandondocument:7cabede30a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f2a0cd77c2"></a>
| Field | Shape |
|---|---|
| <a id="s-a6a8b31a31"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c61ad7270b"></a>`distribution` | "riverhog-protocol" |
| <a id="s-881d4698c2"></a>`module` | "riverhog_protocol" |
| <a id="s-64309115a1"></a>`name` | "ProcessingClaimAbandonDocument" |
| <a id="s-e9841ed690"></a>`unit` | "export" |

## Governing policies

- <a id="pa-02b31155c2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimAbandonDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cf9e29cbd6b36e4c8d78b5db1c4d487678af165a9e09d9f64fa78cc135164371 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "db2136a2c3b36883b975822f17c927e5ac2386f19c8c8c8fa80ac47af99be5f0",
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], reason: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimAbandonDocument",
  "unit": "export"
}
```
