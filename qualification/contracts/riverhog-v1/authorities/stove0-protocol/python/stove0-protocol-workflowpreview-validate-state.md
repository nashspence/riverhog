# stove0_protocol.WorkflowPreview.validate_state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-workflowpreview-validate-state:b978ab3ab4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-55fc2e4d54"></a>
- <a id="s-08f1b580d3"></a>`distribution`: `stove0-protocol`
- <a id="s-26b25c6e93"></a>`module`: `stove0_protocol`
- <a id="s-2f8729d7db"></a>`name`: `validate_state`
- <a id="s-af3b8ea844"></a>`owner`: `stove0_protocol.WorkflowPreview`
- <a id="s-b4ef30b3b6"></a>`unit`: `member`

### Declared structure

- <a id="s-65d4d67929"></a>`kind`: `"method"`
- <a id="s-ce755b6618"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [WorkflowPreview](stove0-protocol-workflowpreview.md)

## Governing policies

- <a id="pa-09dd9a9ab2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.WorkflowPreview.validate_state`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6db7c3b2fa69ad61695364cc6d3de772e4556612321ebf8ceb4fbc97ebcb9323 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "validate_state",
  "owner": "stove0_protocol.WorkflowPreview",
  "unit": "member"
}
```
