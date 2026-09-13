# stove0_protocol.WorkflowPreviewRequestPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreviewrequestpayload:b32657871e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-37a175ba7b"></a>
| Field | Shape |
|---|---|
| <a id="s-5253e3d31a"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-52fcf38390"></a>`distribution` | "stove0-protocol" |
| <a id="s-d14cc8b3a6"></a>`module` | "stove0_protocol" |
| <a id="s-fffd2c399d"></a>`name` | "WorkflowPreviewRequestPayload" |
| <a id="s-eb2797644b"></a>`unit` | "export" |

## Governing policies

- <a id="pa-3f89f6a8ce"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreviewRequestPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2796372706419c0d5cce6cd00373063f677dc92ff17385601ffbd8015c26ad3b -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "48a667fcabde7106f7af21385c2e4c9c5f1a4d2285a7c5171d6a4b9840078ae5",
    "signature": "\"(*, format: Literal['stove0-workflow-preview-request/v1'] = 'stove0-workflow-preview-request/v1', work: stove0_protocol.models.WorkIdentity) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "WorkflowPreviewRequestPayload",
  "unit": "export"
}
```
