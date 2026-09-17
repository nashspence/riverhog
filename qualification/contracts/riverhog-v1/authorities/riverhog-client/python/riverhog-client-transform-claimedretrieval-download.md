# riverhog_client.transform.ClaimedRetrieval.download

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedretrieval-download:39c64c263a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ccbd21d1a2"></a>
- <a id="s-10bf585453"></a>`distribution`: `riverhog-client`
- <a id="s-b9567df5b8"></a>`module`: `riverhog_client.transform`
- <a id="s-65d0d089f6"></a>`name`: `download`
- <a id="s-7e2ebd1dee"></a>`owner`: `riverhog_client.transform.ClaimedRetrieval`
- <a id="s-e157797621"></a>`unit`: `member`

### Declared structure

- <a id="s-3fb7de90d3"></a>`kind`: `"method"`
- <a id="s-0ee4f4141f"></a>`signature`: `"\"(self, artifact: 'ClaimedArtifact', output: 'Path') -> 'int'\""`

## Maintained corroboration

### Related interface records

- [ClaimedRetrieval](riverhog-client-transform-claimedretrieval.md)

## Governing policies

- <a id="pa-d7739f6f87"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedRetrieval.download`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b4fd293b024bcee2e2d1818096ceeb2ab5c7e19a10578be911e5dd36a327f99 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifact: 'ClaimedArtifact', output: 'Path') -> 'int'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "download",
  "owner": "riverhog_client.transform.ClaimedRetrieval",
  "unit": "member"
}
```

</details>
