# riverhog_protocol.CollectionDerivationResponseDocument.validate_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivationres-d8771485db:7f913cd03a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-75df58e9df"></a>
- <a id="s-f33e2fce2f"></a>`distribution`: `riverhog-protocol`
- <a id="s-fbc11e1cdc"></a>`module`: `riverhog_protocol`
- <a id="s-8147b0bbb3"></a>`name`: `validate_identity`
- <a id="s-a520c62332"></a>`owner`: `riverhog_protocol.CollectionDerivationResponseDocument`
- <a id="s-2c80bbeb0f"></a>`unit`: `member`

### Declared structure

- <a id="s-bcbbd24499"></a>`kind`: `"method"`
- <a id="s-6120c4943c"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [CollectionDerivationResponseDocument](riverhog-protocol-collectionderivationresponsedocument.md)

## Governing policies

- <a id="pa-1effdee128"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivationResponseDocument.validate_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9bfda86b760d6723f86c117a9029569c7737b7429f75b9aec2311e5d05b96629 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_identity",
  "owner": "riverhog_protocol.CollectionDerivationResponseDocument",
  "unit": "member"
}
```

</details>
