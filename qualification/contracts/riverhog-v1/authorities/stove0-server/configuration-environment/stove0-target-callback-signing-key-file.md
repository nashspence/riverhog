# STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-server:stove0-target-callback-signing-key-file:9387d19f6a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-b8e9029e16"></a>

| Field | Value |
|---|---|
| <a id="s-6389ef5704"></a>`consumers` | `["stove0-server"]` |
| <a id="s-e5e91bdd9c"></a>`default_expressions` | `["''"]` |
| <a id="s-9ef9b8dc24"></a>`id` | `"stove0-server:environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE"` |
| <a id="s-163925c5a6"></a>`input_shape` | `"environment-string"` |
| <a id="s-f93b3cfeb9"></a>`name` | `"STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE"` |
| <a id="s-338261e172"></a>`owner` | `"stove0-server"` |

## Governing policies

- <a id="pa-7070509a7d"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-server:STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE](../../../evidence/sources/authorities.md#src-67af4f66b9) — [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py::\_secret](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-server` | [some-implementations/stove0/application/server/src/stove0\_core/runtime\_config.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/runtime_config.py) | `values.get(f'{name}_FILE', '')` |

### Machine authority

- `/external_contract/configuration_environment/249`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 893ce7a781f100edfb49c855e00549ab3dbcf1fe22781c14d9096582aa268f88 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "stove0-server:environment:STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE",
  "input_shape": "environment-string",
  "name": "STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE",
  "owner": "stove0-server"
}
```

</details>
