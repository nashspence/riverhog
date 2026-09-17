# riverhog_protocol.CollectionDerivationResponseDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationres-fd859d4e14:117b8ad6d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-095295165f"></a>
- <a id="s-14a5870dd4"></a>`distribution`: `riverhog-protocol`
- <a id="s-f02079f077"></a>`module`: `riverhog_protocol`
- <a id="s-428c596878"></a>`name`: `get`
- <a id="s-6ca915cab9"></a>`owner`: `riverhog_protocol.CollectionDerivationResponseDocument`
- <a id="s-95e5b18bd0"></a>`unit`: `member`

### Declared structure

- <a id="s-81cfa9b102"></a>`kind`: `"method"`
- <a id="s-560566cebd"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [CollectionDerivationResponseDocument](riverhog-protocol-collectionderivationresponsedocument.md)

## Governing policies

- <a id="pa-e8de8e30fb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationResponseDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fc648ddd033be217deb16ecf894291111e07ac09868cb16dc0409eb6a1272e31 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.CollectionDerivationResponseDocument",
  "unit": "member"
}
```

</details>
