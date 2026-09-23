# A_RIVERHOG_AWS_STORE_ARCHIVE_STORAGE_CLASS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-archive-storage-class:f99394d75a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-6d67d56ce0"></a>

| Field | Value |
|---|---|
| <a id="s-1a3e8ba501"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-303afe34c5"></a>`default_expressions` | `["''"]` |
| <a id="s-c1495d444f"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ARCHIVE_STORAGE_CLASS"` |
| <a id="s-a4570fd55b"></a>`input_shape` | `"environment-string"` |
| <a id="s-639a4bef7f"></a>`name` | `"A_RIVERHOG_AWS_STORE_ARCHIVE_STORAGE_CLASS"` |
| <a id="s-a908c852ad"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-0c5b9b7855"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_ARCHIVE_STORAGE_CLASS](../../../evidence/sources/authorities.md#src-91862e7749) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/48`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 795c58d3cb3bc6a3ce9b7eb030a53bba5d7dd1ad8daf7b783fd3e3788221292d -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ARCHIVE_STORAGE_CLASS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_ARCHIVE_STORAGE_CLASS",
  "owner": "a-riverhog-aws-store"
}
```

</details>
