# riverhog_client.processing.ClaimedRetrieval.stream

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval-stream:5b49a773c0 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-240f79cfdc"></a>
- <a id="s-209bf97765"></a>`distribution`: `riverhog-client`
- <a id="s-35b277390b"></a>`module`: `riverhog_client.processing`
- <a id="s-e24e272b60"></a>`name`: `stream`
- <a id="s-9afbd4990f"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-f9bcabea27"></a>`unit`: `member`

### Declared structure

- <a id="s-57706beef3"></a>`kind`: `"method"`
- <a id="s-aa97b7e556"></a>`signature`: `"\"(self, artifact: 'ClaimedArtifact', *, start: 'int' = 0, end: 'int \| None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-7ac72e14ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.stream`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6a45292480c287557693f4609a619e7330e53ba4908c4a5af90ba54b8212c81 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact: 'ClaimedArtifact', *, start: 'int' = 0, end: 'int | None' = None, chunk_size: 'int' = 8388608) -> 'Iterator[Iterator[bytes]]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "stream",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
