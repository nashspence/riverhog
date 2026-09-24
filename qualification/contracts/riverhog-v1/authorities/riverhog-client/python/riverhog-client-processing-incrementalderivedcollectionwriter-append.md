# riverhog_client.processing.IncrementalDerivedCollectionWriter.append

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-incrementalder-52ae86e6e0:d617a43c8a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1dff80d7f1"></a>
- <a id="s-2693e4d164"></a>`distribution`: `riverhog-client`
- <a id="s-9a8ddc89cb"></a>`module`: `riverhog_client.processing`
- <a id="s-20b9cb4900"></a>`name`: `append`
- <a id="s-813b7a64a0"></a>`owner`: `riverhog_client.processing.IncrementalDerivedCollectionWriter`
- <a id="s-47f03c8814"></a>`unit`: `member`

### Declared structure

- <a id="s-4b7cdb252b"></a>`kind`: `"method"`
- <a id="s-4250bd1124"></a>`signature`: `"\"(self, source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""`

## Maintained corroboration

### Related interface records

- [IncrementalDerivedCollectionWriter](riverhog-client-processing-incrementalderivedcollectionwriter.md)

## Governing policies

- <a id="pa-6daa9bd602"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.IncrementalDerivedCollectionWriter.append`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d53377f29c670782adcb6af639d63c367f39ce1394ee2bdaad7a555e3394cd49 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "append",
  "owner": "riverhog_client.processing.IncrementalDerivedCollectionWriter",
  "unit": "member"
}
```

</details>
