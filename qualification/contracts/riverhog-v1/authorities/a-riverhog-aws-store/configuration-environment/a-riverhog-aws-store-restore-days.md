# A_RIVERHOG_AWS_STORE_RESTORE_DAYS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-restore-days:90c0ed5ada -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-5c633c982d"></a>

| Field | Value |
|---|---|
| <a id="s-093b7de81f"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-cb2ce64f5f"></a>`default_expressions` | `["''"]` |
| <a id="s-e6605379f6"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_RESTORE_DAYS"` |
| <a id="s-1160de2df9"></a>`input_shape` | `"environment-string"` |
| <a id="s-340ef8e53f"></a>`name` | `"A_RIVERHOG_AWS_STORE_RESTORE_DAYS"` |
| <a id="s-51770eadf0"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-784a0ac587"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_RESTORE_DAYS](../../../evidence/sources/authorities.md#src-9141fb232e) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/65`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6d219dece13c068948e1f113ab40cdf4fc555464bdcd99e2b88ee039c8762065 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_RESTORE_DAYS",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_RESTORE_DAYS",
  "owner": "a-riverhog-aws-store"
}
```

</details>
