# A_RIVERHOG_AWS_STORE_RESTORE_TIER

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-restore-tier:e89d53f2b9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-90cd77baf7"></a>

| Field | Value |
|---|---|
| <a id="s-bc48d55b1e"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-8f0011fd5b"></a>`default_expressions` | `["''"]` |
| <a id="s-48764ad0e2"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_RESTORE_TIER"` |
| <a id="s-9d81873663"></a>`input_shape` | `"environment-string"` |
| <a id="s-35c021851e"></a>`name` | `"A_RIVERHOG_AWS_STORE_RESTORE_TIER"` |
| <a id="s-5bf462d167"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-4080f5986b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_RESTORE_TIER](../../../evidence/sources/authorities.md#src-7385948284) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/66`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c3ac06a7b649af56bfe0c051db1dba27e8a6f574d0d352154228dfc4b088da62 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_RESTORE_TIER",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_RESTORE_TIER",
  "owner": "a-riverhog-aws-store"
}
```

</details>
