# riverhog_client.CollectionProducer.publish

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-collectionproducer-publish:88ec7fe3e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7cdb961555"></a>
| Field | Shape |
|---|---|
| <a id="s-ae57853e17"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-877589c0bd"></a>`distribution` | "riverhog-client" |
| <a id="s-21615431fd"></a>`module` | "riverhog_client" |
| <a id="s-872120a0a9"></a>`name` | "publish" |
| <a id="s-b9f3dce07e"></a>`owner` | "riverhog_client.CollectionProducer" |
| <a id="s-2eb7e195ed"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.CollectionProducer](riverhog-client-collectionproducer.md)

## Governing policies

- <a id="pa-70c11b4aad"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CollectionProducer.publish`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e27cf8056f19b4ab3331c7e34274fb5e99c3a5a927bd19aaa8bfb0c3a0df518f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, files: 'Iterable[ProducerFile]', *, source_event_id: 'str', source_context: 'Mapping[str, object] | None' = None, provenance_journals: 'Iterable[tuple[str, bytes]] | None' = None, idempotency_key: 'str | None' = None, event_context: 'Mapping[str, object] | None' = None, poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400, progress: 'ReadProgress | None' = None) -> 'ProducedCollection'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "publish",
  "owner": "riverhog_client.CollectionProducer",
  "unit": "member"
}
```
