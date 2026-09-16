# stove0_core.EndpointRegistration

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-endpointregistration:fb6b6547ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-64d81eeac3"></a>
- <a id="s-4061ff675d"></a>`distribution`: `stove0-server`
- <a id="s-062b4aed0c"></a>`module`: `stove0_core`
- <a id="s-d7f746a550"></a>`name`: `EndpointRegistration`
- <a id="s-d1b14516f7"></a>`unit`: `export`

### Declared structure

- <a id="s-3ea49415db"></a>`kind`: `"class"`
- <a id="s-4322c05eac"></a>`signature`: `"\"(base_url: 'str', token: 'str \| None', allow_insecure_http: 'bool', semantic_validator_providers: 'tuple[str, ...]' = ()) -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-4711216a31"></a>`base_url` | `'str'` | `required` |
| <a id="s-47b9cca219"></a>`token` | `'str \| None'` | `required` |
| <a id="s-55e8352986"></a>`allow_insecure_http` | `'bool'` | `required` |
| <a id="s-36c20374c5"></a>`semantic_validator_providers` | `'tuple[str, ...]'` | `()` |

## Governing policies

- <a id="pa-9f4d53ea91"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.EndpointRegistration`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2110e5d12f28b2789070add2debe845ba155a68c78a81ff914a18b50d386ee8c -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "base_url",
        "type": "'str'"
      },
      {
        "default": "required",
        "name": "token",
        "type": "'str | None'"
      },
      {
        "default": "required",
        "name": "allow_insecure_http",
        "type": "'bool'"
      },
      {
        "default": "()",
        "name": "semantic_validator_providers",
        "type": "'tuple[str, ...]'"
      }
    ],
    "kind": "class",
    "signature": "\"(base_url: 'str', token: 'str | None', allow_insecure_http: 'bool', semantic_validator_providers: 'tuple[str, ...]' = ()) -> None\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "EndpointRegistration",
  "unit": "export"
}
```

</details>
