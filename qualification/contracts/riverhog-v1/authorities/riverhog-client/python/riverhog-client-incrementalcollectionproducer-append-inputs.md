# riverhog_client.IncrementalCollectionProducer.append_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionprod-16b9107b5d:289fa067e3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-acf4404862"></a>
- <a id="s-425e814bd9"></a>`distribution`: `riverhog-client`
- <a id="s-f10c2879f3"></a>`module`: `riverhog_client`
- <a id="s-b535a0b96f"></a>`name`: `append_inputs`
- <a id="s-762428b61a"></a>`owner`: `riverhog_client.IncrementalCollectionProducer`
- <a id="s-3e85dfe839"></a>`unit`: `member`

### Declared structure

- <a id="s-2c7e274ba2"></a>`kind`: `"method"`
- <a id="s-8f96435fbb"></a>`signature`: `"\"(self, inputs: 'Sequence[ProducerInput]', *, provenance_journals: 'Mapping[str, bytes] \| None' = None, expected_identities: 'Mapping[str, ProducerArtifactIdentity] \| None' = None) -> 'tuple[ProducerArtifactCustody, ...]'\""`

## Maintained corroboration

### Related interface records

- [IncrementalCollectionProducer](riverhog-client-incrementalcollectionproducer.md)

## Governing policies

- <a id="pa-b87158a2b4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer.append_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9cf3d0ce267696f9e5f48876af1b6055fd00cad516f9d95c28ef21466fb6bd8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, inputs: 'Sequence[ProducerInput]', *, provenance_journals: 'Mapping[str, bytes] | None' = None, expected_identities: 'Mapping[str, ProducerArtifactIdentity] | None' = None) -> 'tuple[ProducerArtifactCustody, ...]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "append_inputs",
  "owner": "riverhog_client.IncrementalCollectionProducer",
  "unit": "member"
}
```

</details>
