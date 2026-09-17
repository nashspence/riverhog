# STOVE0_OPUS_TARGET_TOKEN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-target:stove0-opus-target-token:6454d9b8ad -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-baf83c7110"></a>

| Field | Value |
|---|---|
| <a id="s-a2fa2bd77d"></a>`consumers` | `["stove0-opus-target"]` |
| <a id="s-3fc7fae88b"></a>`default_expressions` | `["unset"]` |
| <a id="s-a54273fb17"></a>`id` | `"stove0-opus-target:environment:STOVE0_OPUS_TARGET_TOKEN"` |
| <a id="s-ea0e7b5fd3"></a>`input_shape` | `"environment-string"` |
| <a id="s-facd7c0dcf"></a>`name` | `"STOVE0_OPUS_TARGET_TOKEN"` |
| <a id="s-a72677a8ab"></a>`owner` | `"stove0-opus-target"` |

## Governing policies

- <a id="pa-0930fdcb6e"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-target:STOVE0_OPUS_TARGET_TOKEN](../../../evidence/sources.md#src-4e4d5f3e40) — [reference/stove0/targets/opus/target/src/stove0\_opus\_target/app.py::\_secret](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/app.py); [reference/stove0/targets/opus/target/src/stove0\_opus\_target/app.py::target\_main](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-target` | [reference/stove0/targets/opus/target/src/stove0\_opus\_target/app.py](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/app.py) | `os.getenv(f'{prefix}_TOKEN')` |
| parser | `stove0-opus-target` | [reference/stove0/targets/opus/target/src/stove0\_opus\_target/app.py](../../../../../../reference/stove0/targets/opus/target/src/stove0_opus_target/app.py) | `os.environ.pop(f'{prefix}_TOKEN')` |

### Machine authority

- `/external_contract/configuration_environment/196`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2baac8cb06687f2537af934b17d695094497bc5002cf5ab406ada58765733ae3 -->

```json
{
  "consumers": [
    "stove0-opus-target"
  ],
  "default_expressions": [
    "unset"
  ],
  "id": "stove0-opus-target:environment:STOVE0_OPUS_TARGET_TOKEN",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_TARGET_TOKEN",
  "owner": "stove0-opus-target"
}
```

</details>
