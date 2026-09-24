# riverhog_protocol.ProcessingCapabilityDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitydocument-get:4ec36a34a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-49636ec8ae"></a>
- <a id="s-e2a18910b0"></a>`distribution`: `riverhog-protocol`
- <a id="s-7438c0f402"></a>`module`: `riverhog_protocol`
- <a id="s-dedbfef500"></a>`name`: `get`
- <a id="s-4cdf9b8ba0"></a>`owner`: `riverhog_protocol.ProcessingCapabilityDocument`
- <a id="s-49a822b3d4"></a>`unit`: `member`

### Declared structure

- <a id="s-c5f97b1375"></a>`kind`: `"method"`
- <a id="s-800e08f536"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingCapabilityDocument](riverhog-protocol-processingcapabilitydocument.md)

## Governing policies

- <a id="pa-a65551f84b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 184dca7e73127e14e539326f994cac693635fef56f576c5893e3a74f3837cd2a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingCapabilityDocument",
  "unit": "member"
}
```

</details>
