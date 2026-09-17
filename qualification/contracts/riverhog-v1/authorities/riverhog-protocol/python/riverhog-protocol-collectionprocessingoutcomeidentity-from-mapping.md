# riverhog_protocol.CollectionProcessingOutcomeIdentity.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionprocessingout-7048ded402:4c137e8f7b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d364ce6f95"></a>
- <a id="s-40e2ddc251"></a>`distribution`: `riverhog-protocol`
- <a id="s-36cbfa120d"></a>`module`: `riverhog_protocol`
- <a id="s-fce0015fc6"></a>`name`: `from_mapping`
- <a id="s-5a341a4bfb"></a>`owner`: `riverhog_protocol.CollectionProcessingOutcomeIdentity`
- <a id="s-b556da2008"></a>`unit`: `member`

### Declared structure

- <a id="s-aaf9c21153"></a>`kind`: `"classmethod"`
- <a id="s-ac2ee8d5e9"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'CollectionProcessingOutcomeIdentity'\""`

## Maintained corroboration

### Related interface records

- [CollectionProcessingOutcomeIdentity](riverhog-protocol-collectionprocessingoutcomeidentity.md)

## Governing policies

- <a id="pa-7cdfdc8ca8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionProcessingOutcomeIdentity.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8b014799114a2ce8e6d9a0cd6c5c06bfe288b67c7d7a84b1735ee8bc0b80bcf -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'CollectionProcessingOutcomeIdentity'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.CollectionProcessingOutcomeIdentity",
  "unit": "member"
}
```

</details>
