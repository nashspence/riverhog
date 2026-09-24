# riverhog_client.processing.ClaimedRetrieval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval:604523297f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6a32752128"></a>
- <a id="s-0732cfc554"></a>`distribution`: `riverhog-client`
- <a id="s-1b4455554e"></a>`module`: `riverhog_client.processing`
- <a id="s-75c0060f40"></a>`name`: `ClaimedRetrieval`
- <a id="s-1b6d692e3e"></a>`unit`: `export`

### Declared structure

- <a id="s-d7d9886fab"></a>`kind`: `"class"`
- <a id="s-83cf6d6e4a"></a>`signature`: `"\"(api: 'ClaimedCollectionApi', *, job: 'Mapping[str, Any]', artifacts: 'Sequence[ClaimedArtifact]', heartbeat: 'Heartbeat \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [read_bytes](riverhog-client-processing-claimedretrieval-read-bytes.md)
- [cleanup_pending](riverhog-client-processing-claimedretrieval-cleanup-pending.md)
- [retry_close](riverhog-client-processing-claimedretrieval-retry-close.md)
- [replace_api](riverhog-client-processing-claimedretrieval-replace-api.md)
- [close](riverhog-client-processing-claimedretrieval-close.md)
- [closed](riverhog-client-processing-claimedretrieval-closed.md)
- [download](riverhog-client-processing-claimedretrieval-download.md)
- [__enter__](riverhog-client-processing-claimedretrieval-enter.md)
- [__exit__](riverhog-client-processing-claimedretrieval-exit.md)
- [renew](riverhog-client-processing-claimedretrieval-renew.md)
- [stream](riverhog-client-processing-claimedretrieval-stream.md)

## Governing policies

- <a id="pa-bea75b1095"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b667e07556126fa8d0b146b5138dccc7962772888100ab220acc73c7cf9a73c -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'ClaimedCollectionApi', *, job: 'Mapping[str, Any]', artifacts: 'Sequence[ClaimedArtifact]', heartbeat: 'Heartbeat | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "ClaimedRetrieval",
  "unit": "export"
}
```

</details>
