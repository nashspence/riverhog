# stove0_observer_protocol.SemanticValidatorBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-semanticvalidatorbinding:4c5498beaa -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9ba83a64c2"></a>
- <a id="s-5f1c9879b4"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-e1b56624a2"></a>`module`: `stove0_observer_protocol`
- <a id="s-e42cbd33e1"></a>`name`: `SemanticValidatorBinding`
- <a id="s-3bb49562ef"></a>`unit`: `export`

### Declared structure

- <a id="s-e8c8010fba"></a>`kind`: `"class"`
- <a id="s-0c0128cbb0"></a>`signature`: `"\"(profile_id: 'str', profile_sha256: 'str', validator: 'FactsSemanticValidator') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-444a988734"></a>`profile_id` | `'str'` | `required` |
| <a id="s-d3f9b32adf"></a>`profile_sha256` | `'str'` | `required` |
| <a id="s-d47d813089"></a>`validator` | `'FactsSemanticValidator'` | `required` |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.SemanticValidatorBinding.from_profile](stove0-observer-protocol-semanticvalidatorbinding-from-profile.md)

## Governing policies

- <a id="pa-8be1f4b9fc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.SemanticValidatorBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9dadbb385be6bdbfaf4a358fa527ceffa447d8900285c0c39cbfb0ca5ab9f328 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "profile_id",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "profile_sha256",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "validator",
        "type": "'FactsSemanticValidator'"
      }
    ],
    "kind": "class",
    "signature": "\"(profile_id: 'str', profile_sha256: 'str', validator: 'FactsSemanticValidator') -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "SemanticValidatorBinding",
  "unit": "export"
}
```
