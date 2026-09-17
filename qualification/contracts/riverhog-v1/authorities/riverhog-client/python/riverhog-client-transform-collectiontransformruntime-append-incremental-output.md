# riverhog_client.transform.CollectionTransformRuntime.append_incremental_output

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-collectiontrans-1ced10413e:71712d4880 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-383669e80a"></a>
- <a id="s-ed01fe2caa"></a>`distribution`: `riverhog-client`
- <a id="s-519ab48971"></a>`module`: `riverhog_client.transform`
- <a id="s-7d4aee37e3"></a>`name`: `append_incremental_output`
- <a id="s-02f1dd3bd4"></a>`owner`: `riverhog_client.transform.CollectionTransformRuntime`
- <a id="s-afd10aca45"></a>`unit`: `member`

### Declared structure

- <a id="s-5b011f982c"></a>`kind`: `"method"`
- <a id="s-8ffec55fc0"></a>`signature`: `"\"(self, writer: 'IncrementalDerivedCollectionWriter', source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""`

## Maintained corroboration

### Related interface records

- [CollectionTransformRuntime](riverhog-client-transform-collectiontransformruntime.md)

## Governing policies

- <a id="pa-11a6bc52a2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.CollectionTransformRuntime.append_incremental_output`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b93b7afc0e563fd2a672e0a39c921bf83cf7a656e06183b68746da8735e1cd04 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, writer: 'IncrementalDerivedCollectionWriter', source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "append_incremental_output",
  "owner": "riverhog_client.transform.CollectionTransformRuntime",
  "unit": "member"
}
```

</details>
