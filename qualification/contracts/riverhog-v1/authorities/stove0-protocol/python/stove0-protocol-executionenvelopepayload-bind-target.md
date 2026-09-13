# stove0_protocol.ExecutionEnvelopePayload.bind_target

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelopepayload-bind-target:6d5f6a3af5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63bbcf61e6"></a>
| Field | Shape |
|---|---|
| <a id="s-e34b75b787"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3f6fc6e652"></a>`distribution` | "stove0-protocol" |
| <a id="s-1120d28903"></a>`module` | "stove0_protocol" |
| <a id="s-0d5ae3f647"></a>`name` | "bind_target" |
| <a id="s-67f8e3bb0e"></a>`owner` | "stove0_protocol.ExecutionEnvelopePayload" |
| <a id="s-375c5906f6"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.ExecutionEnvelopePayload](stove0-protocol-executionenvelopepayload.md)

## Governing policies

- <a id="pa-32c922cc84"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelopePayload.bind_target`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 04d87690a66fee5b54191fb8f4f802fbbe4403c68c98d342d8e5202a056c2984 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "bind_target",
  "owner": "stove0_protocol.ExecutionEnvelopePayload",
  "unit": "member"
}
```
