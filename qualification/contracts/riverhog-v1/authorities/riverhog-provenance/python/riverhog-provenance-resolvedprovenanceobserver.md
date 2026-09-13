# riverhog_provenance.ResolvedProvenanceObserver

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-resolvedprovenanceobserver:74ba903953 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-983af429f4"></a>
| Field | Shape |
|---|---|
| <a id="s-5f0a52c46f"></a>`contract` | additional keys=`fields`, `kind`, `signature` |
| <a id="s-a5e6e94be5"></a>`distribution` | "riverhog-provenance" |
| <a id="s-9b1f820bcf"></a>`module` | "riverhog_provenance" |
| <a id="s-4dd6d21be6"></a>`name` | "ResolvedProvenanceObserver" |
| <a id="s-e7cfa11de9"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_provenance.ResolvedProvenanceObserver.observer_reference](riverhog-provenance-resolvedprovenanceobserver-observer-reference.md)
- [riverhog_provenance.ResolvedProvenanceObserver.create](riverhog-provenance-resolvedprovenanceobserver-create.md)
- [riverhog_provenance.ResolvedProvenanceObserver.as_dict](riverhog-provenance-resolvedprovenanceobserver-as-dict.md)

## Governing policies

- <a id="pa-010f8e5441"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.ResolvedProvenanceObserver`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6ce6fd8c4d18f8aa80cf5dddd4dcd8d186bc33c2a5c1ad78f37f2cd7104441e -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "name",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "metadata",
        "type": "'ProvenanceProviderMetadata'"
      },
      {
        "default": "required",
        "name": "binding",
        "type": "'ProvenanceObserverBinding'"
      },
      {
        "default": "required",
        "name": "contract",
        "type": "'ProvenanceContractBinding'"
      },
      {
        "default": "required",
        "name": "_validator",
        "type": "'Callable[[Mapping[str, Any]], None]'"
      }
    ],
    "kind": "class",
    "signature": "\"(name: 'str', metadata: 'ProvenanceProviderMetadata', binding: 'ProvenanceObserverBinding', contract: 'ProvenanceContractBinding', _validator: 'Callable[[Mapping[str, Any]], None]') -> None\""
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "ResolvedProvenanceObserver",
  "unit": "export"
}
```
