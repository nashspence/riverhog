# A_STOVE0_OPUS_TARGET_STATE_ROOT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-state-root:76949775b8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6b9a1cd0e6"></a>

| Field | Value |
|---|---|
| <a id="s-a1ede7b4cd"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-0e927dd781"></a>`default_expressions` | `["'/var/lib/a-stove0-opus-target'"]` |
| <a id="s-eb324e3723"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_STATE_ROOT"` |
| <a id="s-0147bb6300"></a>`input_shape` | `"environment-string"` |
| <a id="s-b2c273c0ba"></a>`name` | `"A_STOVE0_OPUS_TARGET_STATE_ROOT"` |
| <a id="s-4e30a4d523"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-dd1d16ab5c"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_STATE_ROOT](../../../evidence/sources/authorities.md#src-a9d46e9591) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::target\_main](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_STATE_ROOT', '/var/lib/a-stove0-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/149`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87c7ff433df45422cca3a4bbb0b4fd9957d5116531065d0c3c275858b05970ba -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "'/var/lib/a-stove0-opus-target'"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_STATE_ROOT",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_STATE_ROOT",
  "owner": "a-stove0-opus-target"
}
```

</details>
