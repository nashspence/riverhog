# riverhog_protocol.ProcessingClaimFenceDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimfencedocument:aebabca317 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7e9c3fb5e0"></a>
| Field | Shape |
|---|---|
| <a id="s-c5fc669d00"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-d774b48fcc"></a>`distribution` | "riverhog-protocol" |
| <a id="s-20f0f1e79b"></a>`module` | "riverhog_protocol" |
| <a id="s-145e9046b9"></a>`name` | "ProcessingClaimFenceDocument" |
| <a id="s-153b43a728"></a>`unit` | "export" |

## Governing policies

- <a id="pa-530b1c7343"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimFenceDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4a75e4142b71d8f6e1d2868670d3fe3c3c8dc73d1a40a24cd43aeaa300eb7d3f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "eabbed4a890891c7bfbd4c978da08de3bbd73ad40fb1a9fb9249a8a5ccf814d4",
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimFenceDocument",
  "unit": "export"
}
```
