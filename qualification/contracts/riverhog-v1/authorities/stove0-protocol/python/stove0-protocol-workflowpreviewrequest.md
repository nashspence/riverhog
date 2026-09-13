# stove0_protocol.WorkflowPreviewRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewrequest:31e4186516 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-08bd206d1b"></a>
| Field | Shape |
|---|---|
| <a id="s-3b7ab97f7b"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-658f136d9e"></a>`distribution` | "stove0-protocol" |
| <a id="s-f10178efbf"></a>`module` | "stove0_protocol" |
| <a id="s-5af61f8738"></a>`name` | "WorkflowPreviewRequest" |
| <a id="s-294e1eeecf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.WorkflowPreviewRequest.seal](stove0-protocol-workflowpreviewrequest-seal.md)
- [stove0_protocol.WorkflowPreviewRequest.verify_digest](stove0-protocol-workflowpreviewrequest-verify-digest.md)

## Governing policies

- <a id="pa-8259362854"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 242634b7b909c6351815d8b5099bdaf35e2b33c30ebc22947fb47709d9d8c02b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "afc33b01c4464932d1c696a5d1d691bfc9464b94ff42960587ae8cfcdb140be9",
    "signature": "\"(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity, preview_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreviewRequest",
  "unit": "export"
}
```
