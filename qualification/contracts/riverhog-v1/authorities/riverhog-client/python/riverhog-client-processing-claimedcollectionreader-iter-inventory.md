# riverhog_client.processing.ClaimedCollectionReader.iter_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-7e04ff8833:495c70f6d3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0cfc6228ab"></a>
- <a id="s-13766b403e"></a>`distribution`: `riverhog-client`
- <a id="s-44463dbe11"></a>`module`: `riverhog_client.processing`
- <a id="s-093d3af907"></a>`name`: `iter_inventory`
- <a id="s-47f8bea04a"></a>`owner`: `riverhog_client.processing.ClaimedCollectionReader`
- <a id="s-5cdf723ae9"></a>`unit`: `member`

### Declared structure

- <a id="s-1fc47c47a2"></a>`kind`: `"method"`
- <a id="s-31549645a6"></a>`signature`: `"\"(self, *, include_control: 'bool' = False) -> 'Iterator[ClaimedArtifact]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionReader](riverhog-client-processing-claimedcollectionreader.md)

## Governing policies

- <a id="pa-7d6663e76b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionReader.iter_inventory`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5ccae03f6d19de57f7b66908ec0c71d00a4ea5e8bc15e0d7a63eb74faa22e0d1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, include_control: 'bool' = False) -> 'Iterator[ClaimedArtifact]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "iter_inventory",
  "owner": "riverhog_client.processing.ClaimedCollectionReader",
  "unit": "member"
}
```

</details>
