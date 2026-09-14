# riverhog_protocol.ProducerEvidence.sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-producerevidence-sha256:2bd2f67653 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-73df10e261"></a>
- <a id="s-8cf6155752"></a>`distribution`: `riverhog-protocol`
- <a id="s-1cf0ee3f65"></a>`module`: `riverhog_protocol`
- <a id="s-496fb28d63"></a>`name`: `sha256`
- <a id="s-7ec2d6d28d"></a>`owner`: `riverhog_protocol.ProducerEvidence`
- <a id="s-652295cbad"></a>`unit`: `member`

### Declared structure

- <a id="s-1262c55056"></a>`kind`: `"property"`
- <a id="s-5bc3f7b548"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [riverhog_protocol.ProducerEvidence](riverhog-protocol-producerevidence.md)

## Governing policies

- <a id="pa-e4b0b880f5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProducerEvidence.sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af9646680129b5b551a4871ac67fd5d2d251dd50048b9fc0d381da7721ea7a16 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "sha256",
  "owner": "riverhog_protocol.ProducerEvidence",
  "unit": "member"
}
```
