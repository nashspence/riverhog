# riverhog_client.IncrementalCollectionProducer.finish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionproducer-finish:7a938ca053 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2f3ff640d4"></a>
- <a id="s-70e7babae2"></a>`distribution`: `riverhog-client`
- <a id="s-e362183cb8"></a>`module`: `riverhog_client`
- <a id="s-102ccb811e"></a>`name`: `finish`
- <a id="s-a541e5a376"></a>`owner`: `riverhog_client.IncrementalCollectionProducer`
- <a id="s-595eed6805"></a>`unit`: `member`

### Declared structure

- <a id="s-5522c405de"></a>`kind`: `"method"`
- <a id="s-4a8ba75e69"></a>`signature`: `"\"(self, *, terminal_evidence: 'Mapping[str, bytes]', provenance_journals: 'Mapping[str, bytes] \| None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ProducedCollection'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.IncrementalCollectionProducer](riverhog-client-incrementalcollectionproducer.md)

## Governing policies

- <a id="pa-1abc7006fb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer.finish`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a457ffbfd2fab66c49a8ff6182234f8f2605c4ba91a600cd1bd44a3d6c8f32dd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, terminal_evidence: 'Mapping[str, bytes]', provenance_journals: 'Mapping[str, bytes] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ProducedCollection'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "finish",
  "owner": "riverhog_client.IncrementalCollectionProducer",
  "unit": "member"
}
```
