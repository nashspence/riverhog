# riverhog_protocol.ProcessingCapabilityDocument.validate_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitydoc-f2b6e9cec3:3359409583 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a112a9a5a7"></a>
- <a id="s-b75ea4ba6c"></a>`distribution`: `riverhog-protocol`
- <a id="s-5a090f5be6"></a>`module`: `riverhog_protocol`
- <a id="s-e19f4f849a"></a>`name`: `validate_capability`
- <a id="s-3e01eced95"></a>`owner`: `riverhog_protocol.ProcessingCapabilityDocument`
- <a id="s-dad932e2ea"></a>`unit`: `member`

### Declared structure

- <a id="s-989872a48e"></a>`kind`: `"method"`
- <a id="s-5887b2d294"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ProcessingCapabilityDocument](riverhog-protocol-processingcapabilitydocument.md)

## Governing policies

- <a id="pa-2634b61363"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityDocument.validate_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8067a482c325704a5a800b61a748de81d27057c6fc7859090edba312ab061463 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_capability",
  "owner": "riverhog_protocol.ProcessingCapabilityDocument",
  "unit": "member"
}
```

</details>
