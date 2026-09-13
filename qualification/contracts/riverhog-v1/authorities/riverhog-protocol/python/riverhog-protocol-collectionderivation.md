# riverhog_protocol.CollectionDerivation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivation:a42ac2a412 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-74084c15fc"></a>
| Field | Shape |
|---|---|
| <a id="s-c2545a6799"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-91e082f62c"></a>`distribution` | "riverhog-protocol" |
| <a id="s-3f057600fc"></a>`module` | "riverhog_protocol" |
| <a id="s-1482c7c276"></a>`name` | "CollectionDerivation" |
| <a id="s-3a0bdc594e"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDerivation.as_dict](riverhog-protocol-collectionderivation-as-dict.md)
- [riverhog_protocol.CollectionDerivation.from_mapping](riverhog-protocol-collectionderivation-from-mapping.md)
- [riverhog_protocol.CollectionDerivation.sha256](riverhog-protocol-collectionderivation-sha256.md)
- [riverhog_protocol.CollectionDerivation.to_json_bytes](riverhog-protocol-collectionderivation-to-json-bytes.md)

## Governing policies

- <a id="pa-e36c8a9c05"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivation`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9724e25b458d6752b5d65b01df1978c035ca5e4e987722cf8b3ce91f1ad91dff -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "execution_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "claim_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "fence",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "recipe",
        "type": "'RecipeIdentity'"
      },
      {
        "default": "required",
        "name": "operation",
        "type": "'OperationIdentity'"
      },
      {
        "default": "required",
        "name": "input_set_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "artifact_set_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "execution_envelope_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "execution_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "controller_evidence",
        "type": "'dict[str, JsonValue]'"
      },
      {
        "default": "required",
        "name": "controller_evidence_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "disposition_set",
        "type": "'ArtifactDispositionSetIdentity'"
      }
    ],
    "kind": "class",
    "signature": "\"(execution_id: 'str', claim_id: 'str', fence: 'int', recipe: 'RecipeIdentity', operation: 'OperationIdentity', input_set_sha256: 'str', artifact_set_sha256: 'str', execution_envelope_sha256: 'str', execution_sha256: 'str', controller_evidence: 'dict[str, JsonValue]', controller_evidence_sha256: 'str', disposition_set: 'ArtifactDispositionSetIdentity') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionDerivation",
  "unit": "export"
}
```
