# riverhog_client.transform.ClaimedCollectionRuntime.open_workspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-a315e61ea6:161613f636 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-159b591da4"></a>
- <a id="s-9e01c8fe26"></a>`distribution`: `riverhog-client`
- <a id="s-d53c5fc7de"></a>`module`: `riverhog_client.transform`
- <a id="s-a6ad533331"></a>`name`: `open_workspace`
- <a id="s-daa5672bca"></a>`owner`: `riverhog_client.transform.ClaimedCollectionRuntime`
- <a id="s-14844a5063"></a>`unit`: `member`

### Declared structure

- <a id="s-0d6a5cac69"></a>`kind`: `"method"`
- <a id="s-501f9f0591"></a>`signature`: `"\"(self, root: 'Path', *, assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionRuntime](riverhog-client-transform-claimedcollectionruntime.md)

## Governing policies

- <a id="pa-cf70d3d9fe"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionRuntime.open_workspace`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 823f85eb7e4ee30586a95615ed5eb960ac88fef22f7ef990b7215fbfd7266b05 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, root: 'Path', *, assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "open_workspace",
  "owner": "riverhog_client.transform.ClaimedCollectionRuntime",
  "unit": "member"
}
```
