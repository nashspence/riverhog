# A_STOVE0_OPUS_TARGET_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-stove0-opus-target:a-stove0-opus-target-host:42864d5462 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-opus-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-4abc02115b"></a>

| Field | Value |
|---|---|
| <a id="s-4e0a108175"></a>`consumers` | `["a-stove0-opus-target"]` |
| <a id="s-2d82ee102a"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-f2e09fd5e9"></a>`id` | `"a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_HOST"` |
| <a id="s-40ccec5f8c"></a>`input_shape` | `"environment-string"` |
| <a id="s-ca538f225d"></a>`name` | `"A_STOVE0_OPUS_TARGET_HOST"` |
| <a id="s-79625ca4c2"></a>`owner` | `"a-stove0-opus-target"` |

## Governing policies

- <a id="pa-267bad1e1b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-stove0-opus-target:A_STOVE0_OPUS_TARGET_HOST](../../../evidence/sources/authorities.md#src-a708b15e94) — [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py::\_parser](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-stove0-opus-target` | [some-implementations/stove0/targets/opus/target/src/a\_stove0\_opus\_target/app.py](../../../../../../some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/app.py) | `os.getenv(f'{prefix}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/145`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 65984a9cc1c50d2ee3ac16e3e4c1dc23239fae309a3f44d0fc4150e52ea4c253 -->

```json
{
  "consumers": [
    "a-stove0-opus-target"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "a-stove0-opus-target:environment:A_STOVE0_OPUS_TARGET_HOST",
  "input_shape": "environment-string",
  "name": "A_STOVE0_OPUS_TARGET_HOST",
  "owner": "a-stove0-opus-target"
}
```

</details>
