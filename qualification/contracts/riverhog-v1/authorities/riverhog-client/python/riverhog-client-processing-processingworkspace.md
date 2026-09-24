# riverhog_client.processing.ProcessingWorkspace

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-processingworkspace:63f1eae64b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d6fe142a48"></a>
- <a id="s-a9c3ff34e2"></a>`distribution`: `riverhog-client`
- <a id="s-aacdbd5be7"></a>`module`: `riverhog_client.processing`
- <a id="s-d25ce5de5e"></a>`name`: `ProcessingWorkspace`
- <a id="s-d59a334ebb"></a>`unit`: `export`

### Declared structure

- <a id="s-c8a0740b2d"></a>`kind`: `"class"`
- <a id="s-db654b454a"></a>`signature`: `"\"(root: 'Path', execution_id: 'str', declared_protection: 'DeclaredWorkspaceProtection') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-77bd9bf42b"></a>`root` | `'Path'` | `required` |
| <a id="s-3bb6f1505c"></a>`execution_id` | `'str'` | `required` |
| <a id="s-3aa57884b6"></a>`declared_protection` | `'DeclaredWorkspaceProtection'` | `required` |

## Maintained corroboration

### Related interface records

- [release](riverhog-client-processing-processingworkspace-release.md)
- [resolve](riverhog-client-processing-processingworkspace-resolve.md)
- [__enter__](riverhog-client-processing-processingworkspace-enter.md)
- [__exit__](riverhog-client-processing-processingworkspace-exit.md)
- [open](riverhog-client-processing-processingworkspace-open.md)

## Governing policies

- <a id="pa-84ac7f1f65"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ProcessingWorkspace`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3f0f55c50fc33045c29f43a29f2c72a0a57ef0fa511bbdc5de9c8c90ec2e7e6e -->

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
  "module": "riverhog_client.processing",
  "name": "ProcessingWorkspace",
  "unit": "export"
}
```

</details>
