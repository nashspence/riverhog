# riverhog_protocol.CollectionProcessingOutcomeIdentity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionprocessingout-8ddd033b14:2a9894747e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-772a6e33f8"></a>
| Field | Shape |
|---|---|
| <a id="s-88ff72dda6"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-6abea62f81"></a>`distribution` | "riverhog-protocol" |
| <a id="s-bb9e5ae9b0"></a>`module` | "riverhog_protocol" |
| <a id="s-d61526a9c0"></a>`name` | "CollectionProcessingOutcomeIdentity" |
| <a id="s-87dd47c3d9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionProcessingOutcomeIdentity.as_dict](riverhog-protocol-collectionprocessingoutcomeidentity-as-dict.md)
- [riverhog_protocol.CollectionProcessingOutcomeIdentity.from_mapping](riverhog-protocol-collectionprocessingoutcomeidentity-from-mapping.md)

## Governing policies

- <a id="pa-3fff7ce23e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionProcessingOutcomeIdentity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 919a51cc5126decf021225e895377b247e3087b64d4af35e4b14838689b805d6 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "outcome_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "source_claim_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "output_collection",
        "type": "'CollectionRootIdentity'"
      },
      {
        "default": "required",
        "name": "derivation_sha256",
        "type": "'str'"
      }
    ],
    "kind": "class",
    "signature": "\"(outcome_id: 'str', source_claim_id: 'str', output_collection: 'CollectionRootIdentity', derivation_sha256: 'str') -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "CollectionProcessingOutcomeIdentity",
  "unit": "export"
}
```
