# riverhog_protocol.ProcessingCapabilityDocument.__getitem__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingcapabilitydoc-a99dc22631:3a2f4784e3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-615af193cc"></a>
- <a id="s-9a116cd1d8"></a>`distribution`: `riverhog-protocol`
- <a id="s-58bf0d26c6"></a>`module`: `riverhog_protocol`
- <a id="s-ada06529a3"></a>`name`: `__getitem__`
- <a id="s-ca0cb7f084"></a>`owner`: `riverhog_protocol.ProcessingCapabilityDocument`
- <a id="s-e80926b2db"></a>`unit`: `member`

### Declared structure

- <a id="s-314228a38e"></a>`kind`: `"method"`
- <a id="s-08995edd9d"></a>`signature`: `"\"(self, key: 'str') -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingCapabilityDocument](riverhog-protocol-processingcapabilitydocument.md)

## Governing policies

- <a id="pa-025614d33a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingCapabilityDocument.__getitem__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 308305d2553b5ae22cbbf6167601ea2289909826605f1f701db0999ed5654bac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str') -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "__getitem__",
  "owner": "riverhog_protocol.ProcessingCapabilityDocument",
  "unit": "member"
}
```

</details>
