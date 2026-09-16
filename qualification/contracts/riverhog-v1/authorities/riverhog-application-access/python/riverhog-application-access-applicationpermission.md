# riverhog_application_access.ApplicationPermission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-application-access:riverhog-application-access-applicationpermission:52e027188d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-application-access](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7688f2d3bb"></a>
- <a id="s-e63a7bb217"></a>`distribution`: `riverhog-application-access`
- <a id="s-163632f1f6"></a>`module`: `riverhog_application_access`
- <a id="s-f8fd9a2379"></a>`name`: `ApplicationPermission`
- <a id="s-9483f6fc95"></a>`unit`: `export`

### Declared structure

- <a id="s-5bceda3ae7"></a>`kind`: `"type-alias"`
- <a id="s-1e3f94fd60"></a>`value`: `"typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-transforms:control', 'collection-transforms:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"`

## Governing policies

- <a id="pa-94a28938ee"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-application-access:riverhog_application_access](../../../evidence/sources.md#src-9d9ce5fdac) — `packages/riverhog-application-access/src/riverhog_application_access/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_application_access.ApplicationPermission`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f825d39e21bab23df8a1ca2faa485b271119b3af1dc06f9f43940e2a179d02cf -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-transforms:control', 'collection-transforms:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"
  },
  "distribution": "riverhog-application-access",
  "module": "riverhog_application_access",
  "name": "ApplicationPermission",
  "unit": "export"
}
```

</details>
