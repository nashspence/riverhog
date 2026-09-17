# riverhog_client.transform.ClaimedArtifact.key

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedartifact-key:7297831988 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-83d29487df"></a>
- <a id="s-45d4b0ef45"></a>`distribution`: `riverhog-client`
- <a id="s-4e216fbb25"></a>`module`: `riverhog_client.transform`
- <a id="s-5de23b7eb8"></a>`name`: `key`
- <a id="s-dd6460253e"></a>`owner`: `riverhog_client.transform.ClaimedArtifact`
- <a id="s-9f2ae73a62"></a>`unit`: `member`

### Declared structure

- <a id="s-267848506c"></a>`kind`: `"property"`
- <a id="s-f51d17a4cd"></a>`signature`: `"\"(self) -> 'tuple[int, str]'\""`

## Maintained corroboration

### Related interface records

- [ClaimedArtifact](riverhog-client-transform-claimedartifact.md)

## Governing policies

- <a id="pa-2c9124ffb5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedArtifact.key`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b4eecb6b09e20b8678ae598f9ad28af7ac931a6a4b3e9be6d982e6bb21e5e4f4 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'tuple[int, str]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "key",
  "owner": "riverhog_client.transform.ClaimedArtifact",
  "unit": "member"
}
```

</details>
