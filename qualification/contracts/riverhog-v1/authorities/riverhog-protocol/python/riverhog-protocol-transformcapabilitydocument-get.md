# riverhog_protocol.TransformCapabilityDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-transformcapabilitydocument-get:7680eb3f3f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f459e0caa2"></a>
- <a id="s-5020b5d2c9"></a>`distribution`: `riverhog-protocol`
- <a id="s-5a8dd3d735"></a>`module`: `riverhog_protocol`
- <a id="s-1ec66561a8"></a>`name`: `get`
- <a id="s-ba27ef4a69"></a>`owner`: `riverhog_protocol.TransformCapabilityDocument`
- <a id="s-60c713246f"></a>`unit`: `member`

### Declared structure

- <a id="s-e609e37157"></a>`kind`: `"method"`
- <a id="s-51a8bd651f"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [TransformCapabilityDocument](riverhog-protocol-transformcapabilitydocument.md)

## Governing policies

- <a id="pa-70c20204fa"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.TransformCapabilityDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d1bb74277014c24dcaf1e95b76c805674cc8f95314bce35df539e15a6ae1e20 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.TransformCapabilityDocument",
  "unit": "member"
}
```

</details>
