# riverhog_protocol.collection_upload_raw_digest_summary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-upload-raw-d-5150d2dd8e:137ee89702 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-50ef5ae46a"></a>
- <a id="s-8f913d06e5"></a>`distribution`: `riverhog-protocol`
- <a id="s-a20c9c4f91"></a>`module`: `riverhog_protocol`
- <a id="s-d214383217"></a>`name`: `collection_upload_raw_digest_summary`
- <a id="s-c4ff6460c5"></a>`unit`: `export`

### Declared structure

- <a id="s-3eb500aa7b"></a>`kind`: `"function"`
- <a id="s-fcdf440076"></a>`signature`: `"\"(item: 'CollectionUploadFileIn', constraints: 'CollectionUploadRegistrationConstraintsDocument') -> 'RawSourceDigestSummary \| None'\""`

## Governing policies

- <a id="pa-fd9f28e258"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_upload_raw_digest_summary`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cc793e04bc10e2d682fef47a6b173a0391d4641625b63fd14ab7e35e5248ad41 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(item: 'CollectionUploadFileIn', constraints: 'CollectionUploadRegistrationConstraintsDocument') -> 'RawSourceDigestSummary | None'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_upload_raw_digest_summary",
  "unit": "export"
}
```
