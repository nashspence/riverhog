# riverhog_protocol.ProcessingCapabilityCreateDocument.validate_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitycre-11991955e2:8569a12481 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-74ecc7e84f"></a>
- <a id="s-561fb7ca16"></a>`distribution`: `riverhog-protocol`
- <a id="s-1e0d36065a"></a>`module`: `riverhog_protocol`
- <a id="s-4e244b21fa"></a>`name`: `validate_capability`
- <a id="s-d431eabe92"></a>`owner`: `riverhog_protocol.ProcessingCapabilityCreateDocument`
- <a id="s-e01a726837"></a>`unit`: `member`

### Declared structure

- <a id="s-3949ce5db5"></a>`kind`: `"method"`
- <a id="s-c4dbe04bb2"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ProcessingCapabilityCreateDocument](riverhog-protocol-processingcapabilitycreatedocument.md)

## Governing policies

- <a id="pa-5bff629f05"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityCreateDocument.validate_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 59338d48ff9643d517009b7c26dbc8b096fe8261caf4b948882013674ab1852d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_capability",
  "owner": "riverhog_protocol.ProcessingCapabilityCreateDocument",
  "unit": "member"
}
```

</details>
