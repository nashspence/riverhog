# riverhog_protocol.ProducerEvidence.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-producerevidence-to-json-bytes:203c7e9908 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7a3f25cff9"></a>
- <a id="s-ea1b7b1be0"></a>`distribution`: `riverhog-protocol`
- <a id="s-cbe70bab24"></a>`module`: `riverhog_protocol`
- <a id="s-cef9493a18"></a>`name`: `to_json_bytes`
- <a id="s-3c8b98f2da"></a>`owner`: `riverhog_protocol.ProducerEvidence`
- <a id="s-899fe87af9"></a>`unit`: `member`

### Declared structure

- <a id="s-3538af7379"></a>`kind`: `"method"`
- <a id="s-f448d4c0b0"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ProducerEvidence](riverhog-protocol-producerevidence.md)

## Governing policies

- <a id="pa-d9a63ae383"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProducerEvidence.to_json_bytes`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35e4722a4138cb9a4d5984fea304ab34af4126f23982540b595714b9701a312b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "to_json_bytes",
  "owner": "riverhog_protocol.ProducerEvidence",
  "unit": "member"
}
```
