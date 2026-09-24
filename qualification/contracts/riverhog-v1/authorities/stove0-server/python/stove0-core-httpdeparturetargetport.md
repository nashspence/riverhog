# stove0_core.HttpDepartureTargetPort

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-httpdeparturetargetport:b1520c2e46 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c52a8cd24"></a>
- <a id="s-79adfde4a3"></a>`distribution`: `stove0-server`
- <a id="s-9f09acd31f"></a>`module`: `stove0_core`
- <a id="s-23b11bddf9"></a>`name`: `HttpDepartureTargetPort`
- <a id="s-78a05b1555"></a>`unit`: `export`

### Declared structure

- <a id="s-561e4662d6"></a>`kind`: `"class"`
- <a id="s-fe10f3d015"></a>`signature`: `"\"(registrations: 'dict[str, DepartureEffectClient]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [has_registration](stove0-core-httpdeparturetargetport-has-registration.md)
- [put_effect](stove0-core-httpdeparturetargetport-put-effect.md)

## Governing policies

- <a id="pa-eaf697542f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.HttpDepartureTargetPort`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d1ac2cb2d13870d1177d36cacac1fe34a3a1c6e7b8846d63fdf07ce34440f69e -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(registrations: 'dict[str, DepartureEffectClient]') -> 'None'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "HttpDepartureTargetPort",
  "unit": "export"
}
```

</details>
