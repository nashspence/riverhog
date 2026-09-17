# riverhog_client.transform.TransformWorkspace.open

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-transformworkspace-open:efdd68520b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d532aac98"></a>
- <a id="s-506b1208ca"></a>`distribution`: `riverhog-client`
- <a id="s-b5c9c1219e"></a>`module`: `riverhog_client.transform`
- <a id="s-bd4d002312"></a>`name`: `open`
- <a id="s-3095474a14"></a>`owner`: `riverhog_client.transform.TransformWorkspace`
- <a id="s-424276be11"></a>`unit`: `member`

### Declared structure

- <a id="s-90fde96ae6"></a>`kind`: `"classmethod"`
- <a id="s-7d71359fa6"></a>`signature`: `"\"(cls, root: 'Path', *, execution_id: 'str', assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'\""`

## Maintained corroboration

### Related interface records

- [TransformWorkspace](riverhog-client-transform-transformworkspace.md)

## Governing policies

- <a id="pa-d8f61fce6f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.TransformWorkspace.open`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f883e79ff7767c257475cf84102e270c3ac11b77fca3dc89823dfefbb1402d8c -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, root: 'Path', *, execution_id: 'str', assurance: 'WorkspaceAssurance') -> 'TransformWorkspace'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "open",
  "owner": "riverhog_client.transform.TransformWorkspace",
  "unit": "member"
}
```

</details>
