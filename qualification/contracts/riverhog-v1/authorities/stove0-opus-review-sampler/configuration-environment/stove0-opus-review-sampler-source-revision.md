# STOVE0_OPUS_REVIEW_SAMPLER_SOURCE_REVISION

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:stove0-opus-review-sampler:stove0-opus-review-sampler-source-revision:3b947923ba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-review-sampler](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5cf617c7a1"></a>

| Field | Value |
|---|---|
| <a id="s-3bdfe4a37f"></a>`consumers` | `["stove0-opus-review-sampler"]` |
| <a id="s-f1e8e66179"></a>`default_expressions` | `["'unknown'"]` |
| <a id="s-fc8f9e3eb4"></a>`id` | `"stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_SOURCE_REVISION"` |
| <a id="s-ee45c84e11"></a>`input_shape` | `"environment-string"` |
| <a id="s-e188630524"></a>`name` | `"STOVE0_OPUS_REVIEW_SAMPLER_SOURCE_REVISION"` |
| <a id="s-7c1ae4c87e"></a>`owner` | `"stove0-opus-review-sampler"` |

## Governing policies

- <a id="pa-49e58f951a"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:stove0-opus-review-sampler:STOVE0_OPUS_REVIEW_SAMPLER_SOURCE_REVISION](../../../evidence/sources.md#src-91897194ea) — [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py::main](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `stove0-opus-review-sampler` | [reference/stove0/targets/opus/review-sampler/src/stove0\_opus\_review\_sampler/app.py](../../../../../../reference/stove0/targets/opus/review-sampler/src/stove0_opus_review_sampler/app.py) | `os.getenv(f'{prefix}_SOURCE_REVISION', 'unknown')` |

### Machine authority

- `/external_contract/configuration_environment/186`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bad9e43c29671859e02e2b34736acb1723c398cef02b28690094baa8df4fef24 -->

```json
{
  "consumers": [
    "stove0-opus-review-sampler"
  ],
  "default_expressions": [
    "'unknown'"
  ],
  "id": "stove0-opus-review-sampler:environment:STOVE0_OPUS_REVIEW_SAMPLER_SOURCE_REVISION",
  "input_shape": "environment-string",
  "name": "STOVE0_OPUS_REVIEW_SAMPLER_SOURCE_REVISION",
  "owner": "stove0-opus-review-sampler"
}
```

</details>
