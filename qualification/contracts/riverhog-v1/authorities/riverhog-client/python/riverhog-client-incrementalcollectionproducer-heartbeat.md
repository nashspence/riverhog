# riverhog_client.IncrementalCollectionProducer.heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionprod-63e9ded9ae:1b319ef08f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-98de9f3a70"></a>
- <a id="s-1b26bf57d2"></a>`distribution`: `riverhog-client`
- <a id="s-02d4333999"></a>`module`: `riverhog_client`
- <a id="s-55bf51d147"></a>`name`: `heartbeat`
- <a id="s-59c94d6576"></a>`owner`: `riverhog_client.IncrementalCollectionProducer`
- <a id="s-446aae2799"></a>`unit`: `member`

### Declared structure

- <a id="s-177955e19b"></a>`kind`: `"method"`
- <a id="s-4bcef21e89"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [IncrementalCollectionProducer](riverhog-client-incrementalcollectionproducer.md)

## Governing policies

- <a id="pa-b2aca88764"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer.heartbeat`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 761c57c27ee41fb09e28b6eba8e966137dad35e69e154ea0f8c67afa8c1c610e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "heartbeat",
  "owner": "riverhog_client.IncrementalCollectionProducer",
  "unit": "member"
}
```

</details>
