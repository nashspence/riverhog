# riverhog_client.ApplicationPermission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-applicationpermission:77647a610b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-762665986e"></a>
- <a id="s-9e041794f6"></a>`distribution`: `riverhog-client`
- <a id="s-af2c4f5a25"></a>`module`: `riverhog_client`
- <a id="s-cc42a8146d"></a>`name`: `ApplicationPermission`
- <a id="s-13979a10f9"></a>`unit`: `export`

### Declared structure

- <a id="s-d74da0cc96"></a>`kind`: `"type-alias"`
- <a id="s-8fdbcc2f3c"></a>`value`: `"typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-transforms:control', 'collection-transforms:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"`

## Governing policies

- <a id="pa-23608c5bfb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApplicationPermission`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ab9ddde8a05878f8260ea2e84000d8873d23288199e785e1c053e1a44893674c -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-transforms:control', 'collection-transforms:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ApplicationPermission",
  "unit": "export"
}
```

</details>
