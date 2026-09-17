# riverhog_client.transform.ClaimedCollectionReader

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollectionreader:c6e05261c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4de3906331"></a>
- <a id="s-05f1a9a746"></a>`distribution`: `riverhog-client`
- <a id="s-7fbc75c558"></a>`module`: `riverhog_client.transform`
- <a id="s-6b777f31df"></a>`name`: `ClaimedCollectionReader`
- <a id="s-03372377db"></a>`unit`: `export`

### Declared structure

- <a id="s-2f9a17db00"></a>`kind`: `"class"`
- <a id="s-54e1fff6bb"></a>`signature`: `"\"(api: 'ClaimedCollectionApi', *, inputs: 'Sequence[CollectionRootIdentity]', work_id: 'str', claim_id: 'str', fence: 'int', heartbeat: 'Heartbeat \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [close_retrievals](riverhog-client-transform-claimedcollectionreader-close-retrievals.md)
- [prepare](riverhog-client-transform-claimedcollectionreader-prepare.md)
- [replace_api](riverhog-client-transform-claimedcollectionreader-replace-api.md)
- [iter_inventory](riverhog-client-transform-claimedcollectionreader-iter-inventory.md)

## Governing policies

- <a id="pa-ec062ac831"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionReader`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acac174b3d76f43579b7155bfad7c0dbfe8635539492ff84f9e0a8e381604ccc -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'ClaimedCollectionApi', *, inputs: 'Sequence[CollectionRootIdentity]', work_id: 'str', claim_id: 'str', fence: 'int', heartbeat: 'Heartbeat | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "ClaimedCollectionReader",
  "unit": "export"
}
```

</details>
