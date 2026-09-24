# stove0_core.DepartureEffectService

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-departureeffectservice:b20117b038 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1894bf685"></a>
- <a id="s-52d68cbba5"></a>`distribution`: `stove0-server`
- <a id="s-1033f3a909"></a>`module`: `stove0_core`
- <a id="s-162cf27027"></a>`name`: `DepartureEffectService`
- <a id="s-9f4f158671"></a>`unit`: `export`

### Declared structure

- <a id="s-6e392f42ed"></a>`kind`: `"class"`
- <a id="s-4418475768"></a>`signature`: `"\"(*, catalog: 'DepartureCatalog', riverhog: 'ApiClient', state: 'SqlAlchemyStateStore', targets: 'DepartureTargetPort') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [advance](stove0-core-departureeffectservice-advance.md)
- [get_effect](stove0-core-departureeffectservice-get-effect.md)
- [list_effects](stove0-core-departureeffectservice-list-effects.md)
- [policies](stove0-core-departureeffectservice-policies.md)
- [rebaseline](stove0-core-departureeffectservice-rebaseline.md)

## Governing policies

- <a id="pa-9592edb3ab"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.DepartureEffectService`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 190131d67b8c2f44c4b7190f38b8a41df405f9f2567d19c2cfb8e32c2b21fcb5 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, catalog: 'DepartureCatalog', riverhog: 'ApiClient', state: 'SqlAlchemyStateStore', targets: 'DepartureTargetPort') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "DepartureEffectService",
  "unit": "export"
}
```

</details>
