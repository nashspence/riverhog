# riverhog_client.ProducerArtifactCustody

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-producerartifactcustody:8ef010d86e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3e3d77fa22"></a>
| Field | Shape |
|---|---|
| <a id="s-e4f759ff62"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-8796264bb3"></a>`distribution` | "riverhog-client" |
| <a id="s-cac0886349"></a>`module` | "riverhog_client" |
| <a id="s-fadfc6b693"></a>`name` | "ProducerArtifactCustody" |
| <a id="s-40b56b887c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9774d6361a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ProducerArtifactCustody`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a0dca7bd9d7b87150c27c97f64fe595845049b46240c04548351ce8a58ed232 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "artifact",
        "type": "'ProducerArtifactIdentity'"
      },
      {
        "default": "required",
        "name": "receipt",
        "type": "'CollectionUploadArtifactCustodyReceiptDocument'"
      }
    ],
    "kind": "class",
    "signature": "\"(artifact: 'ProducerArtifactIdentity', receipt: 'CollectionUploadArtifactCustodyReceiptDocument') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ProducerArtifactCustody",
  "unit": "export"
}
```
