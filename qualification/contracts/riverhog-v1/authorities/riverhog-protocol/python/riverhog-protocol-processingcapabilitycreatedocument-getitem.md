# riverhog_protocol.ProcessingCapabilityCreateDocument.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitycre-e68cc3b7ee:6c1d5b6355 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e4b2706a6"></a>
- <a id="s-b20eae7e61"></a>`distribution`: `riverhog-protocol`
- <a id="s-12533b038f"></a>`module`: `riverhog_protocol`
- <a id="s-d5632c07e6"></a>`name`: `__getitem__`
- <a id="s-23ab478b95"></a>`owner`: `riverhog_protocol.ProcessingCapabilityCreateDocument`
- <a id="s-5410235f2c"></a>`unit`: `member`

### Declared structure

- <a id="s-2ac2f5c1a8"></a>`kind`: `"method"`
- <a id="s-33c15c4c3f"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingCapabilityCreateDocument](riverhog-protocol-processingcapabilitycreatedocument.md)

## Governing policies

- <a id="pa-97c9292ed3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityCreateDocument.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc634792846fe57eb399cfccb2d931a3f9ba5ff6c23f3c1ef942b3b052e8b73b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "__getitem__",
  "owner": "riverhog_protocol.ProcessingCapabilityCreateDocument",
  "unit": "member"
}
```

</details>
