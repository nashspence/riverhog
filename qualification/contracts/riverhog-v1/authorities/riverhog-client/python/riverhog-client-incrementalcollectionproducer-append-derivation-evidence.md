# riverhog_client.IncrementalCollectionProducer.append_derivation_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-incrementalcollectionprod-caec16269f:df0cb9b5ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25aeca38b2"></a>
| Field | Shape |
|---|---|
| <a id="s-2f5b9c2468"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-82af551fc2"></a>`distribution` | "riverhog-client" |
| <a id="s-b6c03f23ff"></a>`module` | "riverhog_client" |
| <a id="s-432baf3fd8"></a>`name` | "append_derivation_evidence" |
| <a id="s-a85dad704f"></a>`owner` | "riverhog_client.IncrementalCollectionProducer" |
| <a id="s-0823927dac"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.IncrementalCollectionProducer](riverhog-client-incrementalcollectionproducer.md)

## Governing policies

- <a id="pa-1a67bef16f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.IncrementalCollectionProducer.append_derivation_evidence`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9035e4440cac371d6f756b03774034c40ca57aba38c61425d68d39877f84e99c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, path: 'str', content: 'bytes') -> 'ProducerArtifactCustody | None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "append_derivation_evidence",
  "owner": "riverhog_client.IncrementalCollectionProducer",
  "unit": "member"
}
```
