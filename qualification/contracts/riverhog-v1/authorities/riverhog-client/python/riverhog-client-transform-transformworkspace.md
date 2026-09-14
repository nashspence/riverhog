# riverhog_client.transform.TransformWorkspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-transformworkspace:3897c477e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0036740015"></a>
- <a id="s-164d8f8923"></a>`distribution`: `riverhog-client`
- <a id="s-59e93f1b21"></a>`module`: `riverhog_client.transform`
- <a id="s-81e6b0c2fb"></a>`name`: `TransformWorkspace`
- <a id="s-962c4776c3"></a>`unit`: `export`

### Declared structure

- <a id="s-a40ea62101"></a>`kind`: `"class"`
- <a id="s-1dbacd25ce"></a>`signature`: `"\"(root: 'Path', execution_id: 'str', assurance: 'WorkspaceAssurance') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-6db8a21458"></a>`root` | `'Path'` | `required` |
| <a id="s-ddbe6f07e9"></a>`execution_id` | `'str'` | `required` |
| <a id="s-fa68e4e302"></a>`assurance` | `'WorkspaceAssurance'` | `required` |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.TransformWorkspace.__enter__](riverhog-client-transform-transformworkspace-enter.md)
- [riverhog_client.transform.TransformWorkspace.__exit__](riverhog-client-transform-transformworkspace-exit.md)
- [riverhog_client.transform.TransformWorkspace.open](riverhog-client-transform-transformworkspace-open.md)
- [riverhog_client.transform.TransformWorkspace.release](riverhog-client-transform-transformworkspace-release.md)
- [riverhog_client.transform.TransformWorkspace.resolve](riverhog-client-transform-transformworkspace-resolve.md)

## Governing policies

- <a id="pa-dc33dff81a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.TransformWorkspace`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5418831922a3e113fa2d956ecaf548ed30b4d75e8dcf025d6a99a53cf0e3f82e -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "root",
        "type": "'Path'"
      },
      {
        "default": "required",
        "name": "execution_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "assurance",
        "type": "'WorkspaceAssurance'"
      }
    ],
    "kind": "class",
    "signature": "\"(root: 'Path', execution_id: 'str', assurance: 'WorkspaceAssurance') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "TransformWorkspace",
  "unit": "export"
}
```
