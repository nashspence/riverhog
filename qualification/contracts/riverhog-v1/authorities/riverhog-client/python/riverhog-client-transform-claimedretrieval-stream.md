# riverhog_client.transform.ClaimedRetrieval.stream

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval-stream:c7c4ba68b7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43a9ca7c5f"></a>
- <a id="s-0f09077829"></a>`distribution`: `riverhog-client`
- <a id="s-5638591368"></a>`module`: `riverhog_client.transform`
- <a id="s-df8b6583fa"></a>`name`: `stream`
- <a id="s-3c24ebe83b"></a>`owner`: `riverhog_client.transform.ClaimedRetrieval`
- <a id="s-c8cd2073ff"></a>`unit`: `member`

### Declared structure

- <a id="s-c1a9917487"></a>`kind`: `"method"`
- <a id="s-fa69443864"></a>`signature`: `"\"(self, artifact: 'ClaimedArtifact', *, start: 'int' = 0, end: 'int \| None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-9f7f69b196"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.stream`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8e9a5a8b022f18e8d009dd531afb56309ce907dc2bd600f688bc54fec9e3230e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact: 'ClaimedArtifact', *, start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "stream",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
