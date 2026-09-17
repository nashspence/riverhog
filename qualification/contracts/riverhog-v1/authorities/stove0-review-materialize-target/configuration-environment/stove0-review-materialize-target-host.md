# STOVE0_REVIEW_MATERIALIZE_TARGET_HOST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-review-materialize-target:stove0-review-materialize-target-host:e3a4285239 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-0630da4c2b"></a>

| Field | Value |
|---|---|
| <a id="s-02ae451182"></a>`consumers` | `["stove0-review-materialize-target"]` |
| <a id="s-9b5f9f836f"></a>`default_expressions` | `["'127.0.0.1'"]` |
| <a id="s-8acc528e3b"></a>`id` | `"stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_HOST"` |
| <a id="s-658134addc"></a>`input_shape` | `"environment-string"` |
| <a id="s-39b689166d"></a>`name` | `"STOVE0_REVIEW_MATERIALIZE_TARGET_HOST"` |
| <a id="s-5bd49751e0"></a>`owner` | `"stove0-review-materialize-target"` |

## Governing policies

- <a id="pa-b50d457264"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-review-materialize-target:STOVE0_REVIEW_MATERIALIZE_TARGET_HOST](../../../evidence/sources.md#src-d1bde5c693) — [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py::\_parser](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-review-materialize-target` | [reference/stove0/targets/review/materialize-target/src/stove0\_review\_materialize\_target/app.py](../../../../../../reference/stove0/targets/review/materialize-target/src/stove0_review_materialize_target/app.py) | `os.getenv(f'{PREFIX}_HOST', '127.0.0.1')` |

### Machine authority

- `/external_contract/configuration_environment/199`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3bc577415b41ba76375b41e4c7480abc05323cf508b54a0c8d09949e21699a49 -->

```json
{
  "consumers": [
    "stove0-review-materialize-target"
  ],
  "default_expressions": [
    "'127.0.0.1'"
  ],
  "id": "stove0-review-materialize-target:environment:STOVE0_REVIEW_MATERIALIZE_TARGET_HOST",
  "input_shape": "environment-string",
  "name": "STOVE0_REVIEW_MATERIALIZE_TARGET_HOST",
  "owner": "stove0-review-materialize-target"
}
```

</details>
