# riverhog_protocol.ProcessingClaimDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimdocument-get:67ad5adcbc -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a257abe7ca"></a>
- <a id="s-39740813f0"></a>`distribution`: `riverhog-protocol`
- <a id="s-69e6026551"></a>`module`: `riverhog_protocol`
- <a id="s-0e55281793"></a>`name`: `get`
- <a id="s-82da393d9a"></a>`owner`: `riverhog_protocol.ProcessingClaimDocument`
- <a id="s-9f64d1c1b6"></a>`unit`: `member`

### Declared structure

- <a id="s-8694b0d86b"></a>`kind`: `"method"`
- <a id="s-efe6920143"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimDocument](riverhog-protocol-processingclaimdocument.md)

## Governing policies

- <a id="pa-54ddfdaad2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13a1d42257d790ef91e0fb1a77315ad74119707460f909f7f8931906027fe4ed -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimDocument",
  "unit": "member"
}
```

</details>
