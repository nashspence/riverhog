# riverhog_client.processing.ClaimedRetrieval.download

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretrieval-download:f897709da4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b99cbc086"></a>
- <a id="s-6d9f389d02"></a>`distribution`: `riverhog-client`
- <a id="s-2224ece79f"></a>`module`: `riverhog_client.processing`
- <a id="s-330d119e26"></a>`name`: `download`
- <a id="s-14ac6d1d38"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-bad1a6ac17"></a>`unit`: `member`

### Declared structure

- <a id="s-88f953de3d"></a>`kind`: `"method"`
- <a id="s-feb771c2d5"></a>`signature`: `"\"(self, artifact: 'ClaimedArtifact', output: 'Path') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-4f4ee7d381"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.download`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7eb064a03db096c29261c759e5ec3a69f9afa06121f16bf15ded859326ad3e9b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact: 'ClaimedArtifact', output: 'Path') -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "download",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
