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
- <a id="s-a5e6e94be5"></a>`distribution`: `riverhog-provenance`
- <a id="s-9b1f820bcf"></a>`module`: `riverhog_provenance`
- <a id="s-4dd6d21be6"></a>`name`: `ResolvedProvenanceObserver`
- <a id="s-e7cfa11de9"></a>`unit`: `export`

### Declared structure

- <a id="s-349921fa9f"></a>`kind`: `"class"`
- <a id="s-663678b531"></a>`signature`: `"\"(name: 'str', metadata: 'ProvenanceProviderMetadata', binding: 'ProvenanceObserverBinding', contract: 'ProvenanceContractBinding', _validator: 'Callable[[Mapping[str, Any]], None]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-826aa7ac56"></a>`name` | `'str'` | `required` |
| <a id="s-de2c5db533"></a>`metadata` | `'ProvenanceProviderMetadata'` | `required` |
| <a id="s-0243552fb2"></a>`binding` | `'ProvenanceObserverBinding'` | `required` |
| <a id="s-84af2c6730"></a>`contract` | `'ProvenanceContractBinding'` | `required` |
| <a id="s-60ee95f52a"></a>`_validator` | `'Callable[[Mapping[str, Any]], None]'` | `required` |

## Maintained corroboration

### Related interface records

- [observer_reference](riverhog-provenance-resolvedprovenanceobserver-observer-reference.md)
- [create](riverhog-provenance-resolvedprovenanceobserver-create.md)
- [as_dict](riverhog-provenance-resolvedprovenanceobserver-as-dict.md)

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
