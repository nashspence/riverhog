# riverhog_client.processing.ClaimedArtifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedartifact:42974e6ebb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2e431ca81"></a>
- <a id="s-4c9e6349c6"></a>`distribution`: `riverhog-client`
- <a id="s-00a0d308b6"></a>`module`: `riverhog_client.processing`
- <a id="s-5e1b62803b"></a>`name`: `ClaimedArtifact`
- <a id="s-27a0827236"></a>`unit`: `export`

### Declared structure

- <a id="s-3556f6b7ec"></a>`kind`: `"class"`
- <a id="s-b9a88c60c4"></a>`signature`: `"\"(root: 'CollectionRootIdentity', path: 'str', bytes: 'int', sha256: 'str', control: 'bool' = False) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-bb05ff6cb8"></a>`root` | `'CollectionRootIdentity'` | `required` |
| <a id="s-26442a2bd2"></a>`path` | `'str'` | `required` |
| <a id="s-85ffc47454"></a>`bytes` | `'int'` | `required` |
| <a id="s-988e18796c"></a>`sha256` | `'str'` | `required` |
| <a id="s-4dcac8fe11"></a>`control` | `'bool'` | `False` |

## Maintained corroboration

### Related interface records

- [as_dict](riverhog-client-processing-claimedartifact-as-dict.md)
- [key](riverhog-client-processing-claimedartifact-key.md)

## Governing policies

- <a id="pa-e5b477ad51"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedArtifact`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c2538b2dde7e2e595471aa256e6e42de32a6a740a6f0810fe1f097d9ce762eb6 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "root",
        "type": "'CollectionRootIdentity'"
      },
      {
        "default": "required",
        "name": "path",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "bytes",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "sha256",
        "type": "'str'"
      },
      {
        "default": "False",
        "name": "control",
        "type": "'bool'"
      }
    ],
    "kind": "class",
    "signature": "\"(root: 'CollectionRootIdentity', path: 'str', bytes: 'int', sha256: 'str', control: 'bool' = False) -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "ClaimedArtifact",
  "unit": "export"
}
```

</details>
