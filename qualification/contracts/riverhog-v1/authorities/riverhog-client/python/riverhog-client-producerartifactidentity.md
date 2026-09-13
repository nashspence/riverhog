# riverhog_client.ProducerArtifactIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-producerartifactidentity:7aff00f6aa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-edc2066fd1"></a>
| Field | Shape |
|---|---|
| <a id="s-3afe847c65"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-4e33c1b8c8"></a>`distribution` | "riverhog-client" |
| <a id="s-34f79c737b"></a>`module` | "riverhog_client" |
| <a id="s-f7be4b9f3d"></a>`name` | "ProducerArtifactIdentity" |
| <a id="s-4b9f7f022a"></a>`unit` | "export" |

## Governing policies

- <a id="pa-d34b49e0df"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ProducerArtifactIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3215e52f023899b1a4efd51df2a3b8184c2eb058c9c78ea9cc3bd10ea89344f -->

```json
{
  "contract": {
    "fields": [
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
      }
    ],
    "kind": "class",
    "signature": "\"(path: 'str', bytes: 'int', sha256: 'str') -> None\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "ProducerArtifactIdentity",
  "unit": "export"
}
```
