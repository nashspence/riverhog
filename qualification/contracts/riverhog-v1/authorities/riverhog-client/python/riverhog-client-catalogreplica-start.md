# riverhog_client.CatalogReplica.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogreplica-start:47d773c123 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d002236c4"></a>
- <a id="s-ef5d192168"></a>`distribution`: `riverhog-client`
- <a id="s-314a6e23d9"></a>`module`: `riverhog_client`
- <a id="s-302f7cb423"></a>`name`: `start`
- <a id="s-5db7e72458"></a>`owner`: `riverhog_client.CatalogReplica`
- <a id="s-8566a07051"></a>`unit`: `member`

### Declared structure

- <a id="s-d18394e003"></a>`kind`: `"method"`
- <a id="s-3261486795"></a>`signature`: `"\"(self, api: 'CatalogSyncApi') -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [CatalogReplica](riverhog-client-catalogreplica.md)

## Governing policies

- <a id="pa-8d74ee790b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogReplica.start`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 37555b284793f0e398e13fd2dea2d301da7b3a46613c6e2849c7ee1f68b36e28 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, api: 'CatalogSyncApi') -> 'dict[str, object]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "start",
  "owner": "riverhog_client.CatalogReplica",
  "unit": "member"
}
```

</details>
