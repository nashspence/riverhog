# riverhog_protocol.RetrievalFileReferenceSetDocument.validate_exact_reference_set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-retrievalfilereferences-7da2b3cd0c:6fd3ec7805 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a7350ba79f"></a>
- <a id="s-b5e11e673c"></a>`distribution`: `riverhog-protocol`
- <a id="s-22b57529f2"></a>`module`: `riverhog_protocol`
- <a id="s-6046dc98e1"></a>`name`: `validate_exact_reference_set`
- <a id="s-a6349e3eee"></a>`owner`: `riverhog_protocol.RetrievalFileReferenceSetDocument`
- <a id="s-7536085f91"></a>`unit`: `member`

### Declared structure

- <a id="s-212e9b52e0"></a>`kind`: `"method"`
- <a id="s-4c99ad5c8a"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [RetrievalFileReferenceSetDocument](riverhog-protocol-retrievalfilereferencesetdocument.md)

## Governing policies

- <a id="pa-dc2d3b6258"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RetrievalFileReferenceSetDocument.validate_exact_reference_set`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9cd3fb62d44180bcbc69e3477a0b915995def3c993116b407da775968369176 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_exact_reference_set",
  "owner": "riverhog_protocol.RetrievalFileReferenceSetDocument",
  "unit": "member"
}
```

</details>
