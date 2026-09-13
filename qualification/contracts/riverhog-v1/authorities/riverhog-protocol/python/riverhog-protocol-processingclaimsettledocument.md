# riverhog_protocol.ProcessingClaimSettleDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimsettledocument:fa97ca06cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-730bdee45f"></a>
| Field | Shape |
|---|---|
| <a id="s-2b6a341d87"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-983c6203e0"></a>`distribution` | "riverhog-protocol" |
| <a id="s-620de56e1d"></a>`module` | "riverhog_protocol" |
| <a id="s-5c92771092"></a>`name` | "ProcessingClaimSettleDocument" |
| <a id="s-bc6159f138"></a>`unit` | "export" |

## Governing policies

- <a id="pa-b29df7919f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimSettleDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 94d2ef90fc735c3be037fefb016f1e86a03eceb71d1dac39e2c27e59fc7a4e35 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "682a1ec87a74c11eae2be43f2f99840117d339d2c518bfa188b2e15fc1513100",
    "signature": "'(*, fence: Annotated[int, Ge(ge=1)], output_collection_id: CollectionId, derivation: riverhog_protocol.collection_workflow_transport.CollectionDerivationDocument, outcome: riverhog_protocol.collection_workflow_transport.ProcessingOutcomeBindingDocument | None = None) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "ProcessingClaimSettleDocument",
  "unit": "export"
}
```
