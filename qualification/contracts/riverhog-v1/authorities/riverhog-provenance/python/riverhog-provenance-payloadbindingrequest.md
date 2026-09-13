# riverhog_provenance.PayloadBindingRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-payloadbindingrequest:65deb7f0c8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-80bc5eb5f8"></a>
| Field | Shape |
|---|---|
| <a id="s-40f8d7e6e0"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-8ae1c1c72e"></a>`distribution` | "riverhog-provenance" |
| <a id="s-5367e850bf"></a>`module` | "riverhog_provenance" |
| <a id="s-b9912f6197"></a>`name` | "PayloadBindingRequest" |
| <a id="s-a32ad4cb91"></a>`unit` | "export" |

## Governing policies

- <a id="pa-20f94d470e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.PayloadBindingRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39ca03d2e0865318624863c803678c7313312aa54cc0ae749484549736ca596c -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "None",
        "name": "relative_path",
        "type": "'PathInput | None'"
      },
      {
        "default": "'co_resident_primary_payload'",
        "name": "role",
        "type": "'str'"
      },
      {
        "default": "None",
        "name": "replaces_binding_id",
        "type": "'str | None'"
      },
      {
        "default": "None",
        "name": "note",
        "type": "'str | None'"
      }
    ],
    "kind": "class",
    "signature": "\"(relative_path: 'PathInput | None' = None, role: 'str' = 'co_resident_primary_payload', replaces_binding_id: 'str | None' = None, note: 'str | None' = None) -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "PayloadBindingRequest",
  "unit": "export"
}
```
