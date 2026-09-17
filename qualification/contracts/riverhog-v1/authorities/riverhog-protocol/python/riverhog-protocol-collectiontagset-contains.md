# riverhog_protocol.CollectionTagSet.contains

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagset-contains:54e5665fd5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b531bc906"></a>
- <a id="s-9db24ab3fd"></a>`distribution`: `riverhog-protocol`
- <a id="s-e8487eb2e1"></a>`module`: `riverhog_protocol`
- <a id="s-f160a11003"></a>`name`: `contains`
- <a id="s-de84c5a816"></a>`owner`: `riverhog_protocol.CollectionTagSet`
- <a id="s-63810d29dc"></a>`unit`: `member`

### Declared structure

- <a id="s-2f74ff4c1f"></a>`kind`: `"method"`
- <a id="s-add43c3bca"></a>`signature`: `"\"(self, value: 'str') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagSet](riverhog-protocol-collectiontagset.md)

## Governing policies

- <a id="pa-e1387977a6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSet.contains`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e970f7302fa1aa8174e183d82208e686a886acbf2256090d0e2c5594eab46c6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, value: 'str') -> 'bool'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "contains",
  "owner": "riverhog_protocol.CollectionTagSet",
  "unit": "member"
}
```

</details>
