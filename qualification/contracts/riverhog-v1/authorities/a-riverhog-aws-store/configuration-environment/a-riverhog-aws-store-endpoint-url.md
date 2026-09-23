# A_RIVERHOG_AWS_STORE_ENDPOINT_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-endpoint-url:ef2f0366ba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-add94be491"></a>

| Field | Value |
|---|---|
| <a id="s-d2bd3c7959"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-8b42e9e942"></a>`default_expressions` | `["''"]` |
| <a id="s-b3a8cf981b"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ENDPOINT_URL"` |
| <a id="s-b7e92ef304"></a>`input_shape` | `"environment-string"` |
| <a id="s-66e2078736"></a>`name` | `"A_RIVERHOG_AWS_STORE_ENDPOINT_URL"` |
| <a id="s-263bfe3ca9"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-bf5b3f045b"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_ENDPOINT_URL](../../../evidence/sources/authorities.md#src-2cc81c1c7c) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/54`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00535ba162104e0059948fcde6a95dbe6305018bf171672b1ac56db32b299ec8 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_ENDPOINT_URL",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_ENDPOINT_URL",
  "owner": "a-riverhog-aws-store"
}
```

</details>
