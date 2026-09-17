# riverhog_client.transform.ClaimedRetrieval

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval:6680845240 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-661bb23f0c"></a>
- <a id="s-5758876fac"></a>`distribution`: `riverhog-client`
- <a id="s-1239efac7c"></a>`module`: `riverhog_client.transform`
- <a id="s-19e0fdb4b2"></a>`name`: `ClaimedRetrieval`
- <a id="s-b9911df20f"></a>`unit`: `export`

### Declared structure

- <a id="s-e972b53a47"></a>`kind`: `"class"`
- <a id="s-043bfc1a5a"></a>`signature`: `"\"(api: 'ClaimedCollectionApi', *, job: 'Mapping[str, Any]', artifacts: 'Sequence[ClaimedArtifact]', heartbeat: 'Heartbeat \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [replace_api](riverhog-client-transform-claimedretrieval-replace-api.md)
- [cleanup_pending](riverhog-client-transform-claimedretrieval-cleanup-pending.md)
- [retry_close](riverhog-client-transform-claimedretrieval-retry-close.md)
- [read_bytes](riverhog-client-transform-claimedretrieval-read-bytes.md)
- [close](riverhog-client-transform-claimedretrieval-close.md)
- [closed](riverhog-client-transform-claimedretrieval-closed.md)
- [download](riverhog-client-transform-claimedretrieval-download.md)
- [__enter__](riverhog-client-transform-claimedretrieval-enter.md)
- [__exit__](riverhog-client-transform-claimedretrieval-exit.md)
- [renew](riverhog-client-transform-claimedretrieval-renew.md)
- [stream](riverhog-client-transform-claimedretrieval-stream.md)

## Governing policies

- <a id="pa-a1dc710a70"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7bec2f5ed2a41b9d687fa23aedde2f5f4f8241a10cd34be354bed97a01a81ef0 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'ClaimedCollectionApi', *, job: 'Mapping[str, Any]', artifacts: 'Sequence[ClaimedArtifact]', heartbeat: 'Heartbeat | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "ClaimedRetrieval",
  "unit": "export"
}
```

</details>
