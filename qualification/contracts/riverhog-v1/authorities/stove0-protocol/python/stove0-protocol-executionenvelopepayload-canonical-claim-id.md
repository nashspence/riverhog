# stove0_protocol.ExecutionEnvelopePayload.canonical_claim_id

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-executionenvelopepayload-40bffb5102:b8c55350b8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0f8f77027e"></a>
- <a id="s-7d30b1cae8"></a>`distribution`: `stove0-protocol`
- <a id="s-3f2e2c67e4"></a>`module`: `stove0_protocol`
- <a id="s-965ede04e9"></a>`name`: `canonical_claim_id`
- <a id="s-727b9bed5d"></a>`owner`: `stove0_protocol.ExecutionEnvelopePayload`
- <a id="s-83136fabce"></a>`unit`: `member`

### Declared structure

- <a id="s-92505b6dc5"></a>`kind`: `"classmethod"`
- <a id="s-edf928ce11"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [stove0_protocol.ExecutionEnvelopePayload](stove0-protocol-executionenvelopepayload.md)

## Governing policies

- <a id="pa-dfa212ea55"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ExecutionEnvelopePayload.canonical_claim_id`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e0946fd61fed0533bc649507a4b2f24fc4a11c5553cf01d6a5e83483564e91e -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "canonical_claim_id",
  "owner": "stove0_protocol.ExecutionEnvelopePayload",
  "unit": "member"
}
```
