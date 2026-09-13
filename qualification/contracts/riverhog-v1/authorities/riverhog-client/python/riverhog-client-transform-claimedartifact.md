# riverhog_client.transform.ClaimedArtifact

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedartifact:25310a3ef6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f0435acf79"></a>
| Field | Shape |
|---|---|
| <a id="s-0ac4fcb953"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-2e5618de40"></a>`distribution` | "riverhog-client" |
| <a id="s-cd07b452a3"></a>`module` | "riverhog_client.transform" |
| <a id="s-811ee427d5"></a>`name` | "ClaimedArtifact" |
| <a id="s-a7755924cf"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedArtifact.as_dict](riverhog-client-transform-claimedartifact-as-dict.md)
- [riverhog_client.transform.ClaimedArtifact.key](riverhog-client-transform-claimedartifact-key.md)

## Governing policies

- <a id="pa-7a52f60843"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedArtifact`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32d1c91fe7036bab55d9ded4bcde51ee8d2f00adafdaa2c04fb526eaef4cd958 -->

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
  "module": "riverhog_client.transform",
  "name": "ClaimedArtifact",
  "unit": "export"
}
```
