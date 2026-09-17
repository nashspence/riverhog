# riverhog_protocol.CollectionTagHeadDocument.to_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagheaddocume-798a03dc1e:d432d81ab1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1c04035e89"></a>
- <a id="s-dd27d68806"></a>`distribution`: `riverhog-protocol`
- <a id="s-16f86dbad8"></a>`module`: `riverhog_protocol`
- <a id="s-01f2dadb4b"></a>`name`: `to_json_bytes`
- <a id="s-c02bbcaabe"></a>`owner`: `riverhog_protocol.CollectionTagHeadDocument`
- <a id="s-1227972f2d"></a>`unit`: `member`

### Declared structure

- <a id="s-f6e711818b"></a>`kind`: `"method"`
- <a id="s-e99787a509"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagHeadDocument](riverhog-protocol-collectiontagheaddocument.md)

## Governing policies

- <a id="pa-d4397cf550"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagHeadDocument.to_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bae88d08cc07fc98ac1a47b2d125b25153f13056055be7bccde37693ecc41be1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "to_json_bytes",
  "owner": "riverhog_protocol.CollectionTagHeadDocument",
  "unit": "member"
}
```

</details>
