# riverhog_protocol.CollectionTagSet.discard

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagset-discard:e4f4e3e865 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-07660c5aa2"></a>
- <a id="s-7abfd5f2ba"></a>`distribution`: `riverhog-protocol`
- <a id="s-e6178d4625"></a>`module`: `riverhog_protocol`
- <a id="s-715edeae4d"></a>`name`: `discard`
- <a id="s-9c6a1dba25"></a>`owner`: `riverhog_protocol.CollectionTagSet`
- <a id="s-e87217fcd7"></a>`unit`: `member`

### Declared structure

- <a id="s-23099e27f1"></a>`kind`: `"method"`
- <a id="s-00260e045c"></a>`signature`: `"\"(self, value: 'str') -> 'CollectionTagSet'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagSet](riverhog-protocol-collectiontagset.md)

## Governing policies

- <a id="pa-d76cd3ecf9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagSet.discard`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12ada932fd16c4c7878088d11f9ed73799934a4eeb8b393cbe043c58630e8156 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, value: 'str') -> 'CollectionTagSet'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "discard",
  "owner": "riverhog_protocol.CollectionTagSet",
  "unit": "member"
}
```

</details>
