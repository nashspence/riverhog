# riverhog_storage_adapter_protocol.WriteSegmentPage.canonical_segments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-writese-4eb919a677:15c6ab2593 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c735a0ef0"></a>
- <a id="s-548065ad86"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-368d4a87ec"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-e8e769f703"></a>`name`: `canonical_segments`
- <a id="s-1c0af1d29a"></a>`owner`: `riverhog_storage_adapter_protocol.WriteSegmentPage`
- <a id="s-b5b340c448"></a>`unit`: `member`

### Declared structure

- <a id="s-bb613106be"></a>`kind`: `"classmethod"`
- <a id="s-caedc13a4f"></a>`signature`: `"\"(cls, value: 'tuple[WriteSegmentReceipt, ...]') -> 'tuple[WriteSegmentReceipt, ...]'\""`

## Maintained corroboration

### Related interface records

- [WriteSegmentPage](riverhog-storage-adapter-protocol-writesegmentpage.md)

## Governing policies

- <a id="pa-74eee35712"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.WriteSegmentPage.canonical_segments`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6b538c77b9a191f76f44d26fcf586ca4898396fb148b4f67c778466e930df372 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[WriteSegmentReceipt, ...]') -> 'tuple[WriteSegmentReceipt, ...]'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "canonical_segments",
  "owner": "riverhog_storage_adapter_protocol.WriteSegmentPage",
  "unit": "member"
}
```

</details>
