# riverhog_client.CollectionProducer.publish_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-collectionproducer-publish-inputs:63c43e6e5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2aed75a18d"></a>
| Field | Shape |
|---|---|
| <a id="s-08446b7bc7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-38faf15651"></a>`distribution` | "riverhog-client" |
| <a id="s-18e90e9edd"></a>`module` | "riverhog_client" |
| <a id="s-bf42cf0921"></a>`name` | "publish_inputs" |
| <a id="s-9961bd4532"></a>`owner` | "riverhog_client.CollectionProducer" |
| <a id="s-b38db32a44"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.CollectionProducer](riverhog-client-collectionproducer.md)

## Governing policies

- <a id="pa-9ec04e4bda"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CollectionProducer.publish_inputs`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19c0e7631ad08a9de3311c71a86592b5d065ee55bcd2e8850e021df46ce39484 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, files: 'Iterable[ProducerInput]', *, source_event_id: 'str', source_context: 'Mapping[str, object] | None' = None, provenance_journals: 'Iterable[tuple[str, bytes]] | None' = None, idempotency_key: 'str | None' = None, event_context: 'Mapping[str, object] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400, progress: 'ReadProgress | None' = None) -> 'ProducedCollection'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "publish_inputs",
  "owner": "riverhog_client.CollectionProducer",
  "unit": "member"
}
```
