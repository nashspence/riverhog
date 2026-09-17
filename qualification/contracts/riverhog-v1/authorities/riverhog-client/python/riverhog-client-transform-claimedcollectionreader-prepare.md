# riverhog_client.transform.ClaimedCollectionReader.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-bfc846e249:9fe750d10c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fed2593c3c"></a>
- <a id="s-460a6e0a59"></a>`distribution`: `riverhog-client`
- <a id="s-96993fb1f4"></a>`module`: `riverhog_client.transform`
- <a id="s-58fd7c8379"></a>`name`: `prepare`
- <a id="s-11506c6560"></a>`owner`: `riverhog_client.transform.ClaimedCollectionReader`
- <a id="s-2139607fdc"></a>`unit`: `member`

### Declared structure

- <a id="s-4a1947f1e3"></a>`kind`: `"method"`
- <a id="s-3ea4042239"></a>`signature`: `"\"(self, artifacts: 'Sequence[ClaimedArtifact] \| None' = None, *, lease_seconds: 'int' = 1800, restore_policy: 'RetrievalPolicy' = 'available-only', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ClaimedRetrieval'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionReader](riverhog-client-transform-claimedcollectionreader.md)

## Governing policies

- <a id="pa-6ce53a243e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionReader.prepare`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06e2e4f0bd48360f94bb153396ec428c012aea05da8e0697f58d021d0f4b8091 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, artifacts: 'Sequence[ClaimedArtifact] | None' = None, *, lease_seconds: 'int' = 1800, restore_policy: 'RetrievalPolicy' = 'available-only', poll_seconds: 'float' = 2.0, timeout_seconds: 'float' = 86400) -> 'ClaimedRetrieval'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "prepare",
  "owner": "riverhog_client.transform.ClaimedCollectionReader",
  "unit": "member"
}
```

</details>
