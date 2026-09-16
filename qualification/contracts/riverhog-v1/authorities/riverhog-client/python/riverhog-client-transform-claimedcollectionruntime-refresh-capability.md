# riverhog_client.transform.ClaimedCollectionRuntime.refresh_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-b03e532ec9:f1b76271af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-abab014977"></a>
- <a id="s-3fe2ad79bf"></a>`distribution`: `riverhog-client`
- <a id="s-3e77a3fdd9"></a>`module`: `riverhog_client.transform`
- <a id="s-d9f32d039a"></a>`name`: `refresh_capability`
- <a id="s-b74bf06a1d"></a>`owner`: `riverhog_client.transform.ClaimedCollectionRuntime`
- <a id="s-712f384693"></a>`unit`: `member`

### Declared structure

- <a id="s-41b805e4e1"></a>`kind`: `"method"`
- <a id="s-47bb2e93f2"></a>`signature`: `"\"(self, capability_token: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionRuntime](riverhog-client-transform-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-17de6d91bf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntime.refresh_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8128801ef6025795b147a50e843be2569a7f809f8617d09122aa7eaa54f1b951 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, capability_token: 'str') -> 'None'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "refresh_capability",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntime",
  "unit": "member"
}
```

</details>
