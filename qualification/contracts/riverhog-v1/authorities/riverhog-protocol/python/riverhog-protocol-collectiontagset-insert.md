# riverhog_protocol.CollectionTagSet.insert

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagset-insert:037506da35 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b853aad3a"></a>
- <a id="s-44c0b37b7e"></a>`distribution`: `riverhog-protocol`
- <a id="s-58a4c5236e"></a>`module`: `riverhog_protocol`
- <a id="s-9074573c85"></a>`name`: `insert`
- <a id="s-f81e962a7c"></a>`owner`: `riverhog_protocol.CollectionTagSet`
- <a id="s-0a5264a727"></a>`unit`: `member`

### Declared structure

- <a id="s-58de10d5c2"></a>`kind`: `"method"`
- <a id="s-4f117a50fb"></a>`signature`: `"\"(self, value: 'str') -> 'CollectionTagSet'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagSet](riverhog-protocol-collectiontagset.md)

## Governing policies

- <a id="pa-8817b9512d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSet.insert`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5efeaf83241c6f1aa3dc4a6856662dd318fc29ffcc3bc42c35474413b5a7285a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, value: 'str') -> 'CollectionTagSet'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "insert",
  "owner": "riverhog_protocol.CollectionTagSet",
  "unit": "member"
}
```

</details>
