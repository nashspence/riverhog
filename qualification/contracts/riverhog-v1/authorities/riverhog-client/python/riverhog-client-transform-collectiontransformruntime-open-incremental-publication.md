# riverhog_client.transform.CollectionTransformRuntime.open_incremental_publication

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-4e7863dcf8:019c37b0d6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee838942bc"></a>
- <a id="s-b6eabe1ff9"></a>`distribution`: `riverhog-client`
- <a id="s-47b6d77383"></a>`module`: `riverhog_client.transform`
- <a id="s-b45a9a23ec"></a>`name`: `open_incremental_publication`
- <a id="s-34bb6dc573"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-c3a7f45cdc"></a>`unit`: `member`

### Declared structure

- <a id="s-6e907ef88a"></a>`kind`: `"method"`
- <a id="s-3226dce18c"></a>`signature`: `"\"(self, *, execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] \| None' = None) -> 'IncrementalDerivedCollectionWriter'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-64eccfc602"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.open_incremental_publication`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0a1688908aebdd304e11ac35446779e7b7fe07ac63b5356d299a52587594bbbf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, execution_envelope_sha256: 'str', source_context: 'Mapping[str, object] | None' = None) -> 'IncrementalDerivedCollectionWriter'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "open_incremental_publication",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
