# riverhog_client.processing.ClaimedRetrieval.read_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedretriev-12d3d022a8:5ab3873ad8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7074a81a1e"></a>
- <a id="s-0950decad8"></a>`distribution`: `riverhog-client`
- <a id="s-48a2a97321"></a>`module`: `riverhog_client.processing`
- <a id="s-91fbe9b12c"></a>`name`: `read_bytes`
- <a id="s-792f355587"></a>`owner`: `riverhog_client.processing.ClaimedRetrieval`
- <a id="s-bc520c1c43"></a>`unit`: `member`

### Declared structure

- <a id="s-7b5517150d"></a>`kind`: `"method"`
- <a id="s-511f19602a"></a>`signature`: `"\"(self, artifact: 'ClaimedArtifact', *, maximum_bytes: 'int') -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-processing-claimedretrieval.md)

## Governing policies

- <a id="pa-b1bfca7ee9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedRetrieval.read_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3ab296052e0ebe54a286fece859a4751d760ef06c30031181fcc971292e2441d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact: 'ClaimedArtifact', *, maximum_bytes: 'int') -> 'bytes'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "read_bytes",
  "owner": "riverhog_client.processing.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
