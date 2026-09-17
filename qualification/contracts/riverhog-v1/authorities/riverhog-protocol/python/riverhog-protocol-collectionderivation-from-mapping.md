# riverhog_protocol.CollectionDerivation.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivation-from-mapping:a65d427f97 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e6c6ee20c4"></a>
- <a id="s-ff6363661f"></a>`distribution`: `riverhog-protocol`
- <a id="s-8fa00fd9f8"></a>`module`: `riverhog_protocol`
- <a id="s-3aa65a1f0e"></a>`name`: `from_mapping`
- <a id="s-39160ff0aa"></a>`owner`: `riverhog_protocol.CollectionDerivation`
- <a id="s-aeff24d2c9"></a>`unit`: `member`

### Declared structure

- <a id="s-2725c2e163"></a>`kind`: `"classmethod"`
- <a id="s-059ae6a367"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'CollectionDerivation'\""`

## Maintained corroboration

### Related interface records

- [CollectionDerivation](riverhog-protocol-collectionderivation.md)

## Governing policies

- <a id="pa-5d2b35f3b2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivation.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b9b6905e264079c7a97a957daf5f8af9eaf5255e33c1354ac0fc85189711c3f0 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'CollectionDerivation'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.CollectionDerivation",
  "unit": "member"
}
```

</details>
