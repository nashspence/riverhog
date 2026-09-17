# riverhog_protocol.ProcessingOutcomeBindingDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingoutcomebindin-a95bfe14e8:930b57746e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3615fed522"></a>
- <a id="s-98255498c6"></a>`distribution`: `riverhog-protocol`
- <a id="s-f4cf4046bb"></a>`module`: `riverhog_protocol`
- <a id="s-fe06edde19"></a>`name`: `get`
- <a id="s-1683273aea"></a>`owner`: `riverhog_protocol.ProcessingOutcomeBindingDocument`
- <a id="s-9b94ee3849"></a>`unit`: `member`

### Declared structure

- <a id="s-8f7951603e"></a>`kind`: `"method"`
- <a id="s-730c1c5597"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingOutcomeBindingDocument](riverhog-protocol-processingoutcomebindingdocument.md)

## Governing policies

- <a id="pa-44fbb3edd9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingOutcomeBindingDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 104afdaa2e0f6c21eab28df90510d3ad4601c4810eaf8019670da776ff87ca61 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingOutcomeBindingDocument",
  "unit": "member"
}
```

</details>
