# A_STOVE0_OPUS_TARGET_WORKSPACE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-workspace:652d892898 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1b4b47f8eb"></a>

| Field | Value |
|---|---|
| <a id="s-fc4c1b6f7b"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-ad70defaf4"></a>`default_expressions` | `["'/run/a-stove0-opus-target'"]` |
| <a id="s-142147407b"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_WORKSPACE"` |
| <a id="s-63ba3637de"></a>`input_shape` | `"environment-string"` |
| <a id="s-56e871d5e4"></a>`name` | `"A_STOVE0_OPUS_TARGET_WORKSPACE"` |
| <a id="s-230d2f78c0"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-50645576f9"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_WORKSPACE](../../../evidence/sources/authorities.md#src-216c3f368a) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::target\_main](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_WORKSPACE', '/run/a-stove0-opus-target')` |

### Machine authority

- `/external_contract/configuration_environment/152`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5af016e50bd49a856c7266f7d0ae713f0df6f7bdda922c060b9d132d53a361d0 -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "'/run/a-stove0-opus-target'"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_WORKSPACE",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_WORKSPACE",
  "owner": "a-stove0-opus-target"
}
```

</details>
