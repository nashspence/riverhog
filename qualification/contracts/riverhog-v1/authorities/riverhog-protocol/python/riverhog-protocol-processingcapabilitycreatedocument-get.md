# riverhog_protocol.ProcessingCapabilityCreateDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitycre-ba1c997401:8788d3d827 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d71ca88772"></a>
- <a id="s-64223c0d1d"></a>`distribution`: `riverhog-protocol`
- <a id="s-4f9e524d8c"></a>`module`: `riverhog_protocol`
- <a id="s-c112d4440c"></a>`name`: `get`
- <a id="s-57547bff50"></a>`owner`: `riverhog_protocol.ProcessingCapabilityCreateDocument`
- <a id="s-fb91756d3a"></a>`unit`: `member`

### Declared structure

- <a id="s-36d765bf8d"></a>`kind`: `"method"`
- <a id="s-2a5a97a75b"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingCapabilityCreateDocument](riverhog-protocol-processingcapabilitycreatedocument.md)

## Governing policies

- <a id="pa-cdbfc00a90"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityCreateDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ddb06aef75cfca6d81aba515db98e2c1a977b99f4c4dd172cfe7e9dba225a5e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingCapabilityCreateDocument",
  "unit": "member"
}
```

</details>
