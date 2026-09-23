# A_RIVERHOG_AWS_STORE_TCP_KEEPALIVE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-aws-store:a-riverhog-aws-store-tcp-keepalive:6da1a091f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-aws-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-a4749ac7f3"></a>

| Field | Value |
|---|---|
| <a id="s-a43b0da62e"></a>`consumers` | `["a-riverhog-aws-store"]` |
| <a id="s-14e364ccc3"></a>`default_expressions` | `["''"]` |
| <a id="s-bff0e51f69"></a>`id` | `"a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_TCP_KEEPALIVE"` |
| <a id="s-9ac8b2959d"></a>`input_shape` | `"environment-string"` |
| <a id="s-45655e8c1b"></a>`name` | `"A_RIVERHOG_AWS_STORE_TCP_KEEPALIVE"` |
| <a id="s-6c4637bf18"></a>`owner` | `"a-riverhog-aws-store"` |

## Governing policies

- <a id="pa-b076c6bd6f"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-aws-store:A_RIVERHOG_AWS_STORE_TCP_KEEPALIVE](../../../evidence/sources/authorities.md#src-3fa426ba62) — [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-aws-store` | [some-implementations/riverhog/storage/aws/src/a\_riverhog\_aws\_store/app.py](../../../../../../some-implementations/riverhog/storage/aws/src/a_riverhog_aws_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/73`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b17073f928c74ec9a7cadfdc1582db9ddf0dbdd38bcf9c47e796309e4cc007d7 -->

```json
{
  "consumers": [
    "a-riverhog-aws-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-aws-store:environment:A_RIVERHOG_AWS_STORE_TCP_KEEPALIVE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_AWS_STORE_TCP_KEEPALIVE",
  "owner": "a-riverhog-aws-store"
}
```

</details>
