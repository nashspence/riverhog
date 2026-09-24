# riverhog_client.ApplicationPermission

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-applicationpermission:77647a610b -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-8fdbcc2f3c"></a>`value`: `"typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-processing:control', 'collection-processing:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"`

## Governing policies

- <a id="pa-23608c5bfb"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.ApplicationPermission`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 141d3a4428701b975b1debb1aecf891bc4072688c89ef53ef4a4a83181b91791 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Literal['*', 'catalog:read', 'retrieval:manage', 'collections:create', 'collection-descriptions:manage', 'collection-processing:control', 'collection-processing:execute', 'collection-tags:manage', 'collections:delete', 'archives:read', 'archives:manage', 'keys:manage', 'quotas:manage', 'events:read', 'events:read_all', 'provenance:read', 'provenance:export']"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ApplicationPermission",
  "unit": "export"
}
```

</details>
