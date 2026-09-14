# riverhog_client.transform.IncrementalDerivedCollectionWriter.append

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-incrementalderi-71811a3be5:d93e176c84 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-21505125a9"></a>
- <a id="s-b1d647e003"></a>`distribution`: `riverhog-client`
- <a id="s-d18f1dd972"></a>`module`: `riverhog_client.transform`
- <a id="s-ed72d94e51"></a>`name`: `append`
- <a id="s-ce7a09528e"></a>`owner`: `riverhog_client.transform.IncrementalDerivedCollectionWriter`
- <a id="s-6e28122633"></a>`unit`: `member`

### Declared structure

- <a id="s-a6812bcfbd"></a>`kind`: `"method"`
- <a id="s-563c1ebcd8"></a>`signature`: `"\"(self, source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""`

## Maintained corroboration

### Related interface records

- [IncrementalDerivedCollectionWriter](riverhog-client-transform-incrementalderivedcollectionwriter.md)

## Governing policies

- <a id="pa-76cade3363"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.IncrementalDerivedCollectionWriter.append`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b594efd90dcd3dc594b1936fdbf10596803e817a5479affdb46aa34550922894 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, source: 'ProducerInput', *, identity: 'ProducerArtifactIdentity') -> 'tuple[ProducerArtifactCustody, ...]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "append",
  "owner": "riverhog_client.transform.IncrementalDerivedCollectionWriter",
  "unit": "member"
}
```
