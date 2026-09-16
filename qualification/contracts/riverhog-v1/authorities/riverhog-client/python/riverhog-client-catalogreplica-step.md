# riverhog_client.CatalogReplica.step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica-step:d8c06a1be9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-898cfc2d9e"></a>
- <a id="s-4a971d9cec"></a>`distribution`: `riverhog-client`
- <a id="s-c4088d9c7d"></a>`module`: `riverhog_client`
- <a id="s-fcf85e7d92"></a>`name`: `step`
- <a id="s-bf662fdc06"></a>`owner`: `riverhog_client.CatalogReplica`
- <a id="s-eb6eaf4a02"></a>`unit`: `member`

### Declared structure

- <a id="s-a55c0fa6df"></a>`kind`: `"method"`
- <a id="s-5d33a5de0b"></a>`signature`: `"\"(self, api: 'CatalogSyncApi', *, limit: 'int' = 100) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [CatalogReplica](riverhog-client-catalogreplica.md)

## Governing policies

- <a id="pa-4f80e9ea37"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica.step`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34d0536080a19de746aa615decfe968b085338def96569c224478701f3bf5776 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'CatalogSyncApi', *, limit: 'int' = 100) -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "step",
  "owner": "riverhog_client.CatalogReplica",
  "unit": "member"
}
```

</details>
