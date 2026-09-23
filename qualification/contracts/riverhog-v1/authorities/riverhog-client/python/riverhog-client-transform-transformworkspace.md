# riverhog_client.transform.TransformWorkspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-transformworkspace:3897c477e3 -->

Exact externally visible contract owned by this contract element.

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
- <a id="s-1dbacd25ce"></a>`signature`: `"\"(root: 'Path', execution_id: 'str', declared_protection: 'DeclaredWorkspaceProtection') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-6db8a21458"></a>`root` | `'Path'` | `required` |
| <a id="s-ddbe6f07e9"></a>`execution_id` | `'str'` | `required` |
| <a id="s-fa68e4e302"></a>`declared_protection` | `'DeclaredWorkspaceProtection'` | `required` |

## Maintained corroboration

### Related interface records

- [__enter__](riverhog-client-transform-transformworkspace-enter.md)
- [__exit__](riverhog-client-transform-transformworkspace-exit.md)
- [open](riverhog-client-transform-transformworkspace-open.md)
- [release](riverhog-client-transform-transformworkspace-release.md)
- [resolve](riverhog-client-transform-transformworkspace-resolve.md)

## Governing policies

- <a id="pa-dc33dff81a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources/authorities.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.TransformWorkspace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a235d5a6a098516fff41cd829bdbd855c0de2de18bd0867bf3eeab0cd96671a3 -->

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
        "name": "declared_protection",
        "type": "'DeclaredWorkspaceProtection'"
      }
    ],
    "kind": "class",
    "signature": "\"(root: 'Path', execution_id: 'str', declared_protection: 'DeclaredWorkspaceProtection') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "TransformWorkspace",
  "unit": "export"
}
```

</details>
