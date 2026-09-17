# riverhog_protocol.ProcessingOutcomeIdentityDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingoutcomeidenti-2a7086568b:dc76957210 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a3046e7837"></a>
- <a id="s-ac9f70f9af"></a>`distribution`: `riverhog-protocol`
- <a id="s-23b2a15783"></a>`module`: `riverhog_protocol`
- <a id="s-29566eeca1"></a>`name`: `get`
- <a id="s-b994ab5885"></a>`owner`: `riverhog_protocol.ProcessingOutcomeIdentityDocument`
- <a id="s-da80f56107"></a>`unit`: `member`

### Declared structure

- <a id="s-f326b9b4d3"></a>`kind`: `"method"`
- <a id="s-f6283bdc84"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingOutcomeIdentityDocument](riverhog-protocol-processingoutcomeidentitydocument.md)

## Governing policies

- <a id="pa-c60bf04a49"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingOutcomeIdentityDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffbf3a239c2519f276e59f1840f4a80094fa1da195a725a6cbc447a462e24c1b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingOutcomeIdentityDocument",
  "unit": "member"
}
```

</details>
