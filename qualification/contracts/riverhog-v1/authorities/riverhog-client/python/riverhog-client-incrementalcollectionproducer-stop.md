# riverhog_client.IncrementalCollectionProducer.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionproducer-stop:2c4848393f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-36b56cb446"></a>
| Field | Shape |
|---|---|
| <a id="s-7b2daf1331"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f4540a7137"></a>`distribution` | "riverhog-client" |
| <a id="s-08e1d09f79"></a>`module` | "riverhog_client" |
| <a id="s-461e073d01"></a>`name` | "stop" |
| <a id="s-427c6d4043"></a>`owner` | "riverhog_client.IncrementalCollectionProducer" |
| <a id="s-29b5373b96"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.IncrementalCollectionProducer](riverhog-client-incrementalcollectionproducer.md)

## Governing policies

- <a id="pa-5610742c18"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer.stop`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a73456e5b1fb3e906752b02fe16592f81110a7ef8aac5ef38164feb57ef71cd3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "stop",
  "owner": "riverhog_client.IncrementalCollectionProducer",
  "unit": "member"
}
```
