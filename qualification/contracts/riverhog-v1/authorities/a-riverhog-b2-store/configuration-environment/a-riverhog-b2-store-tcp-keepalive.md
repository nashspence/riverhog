# A_RIVERHOG_B2_STORE_TCP_KEEPALIVE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-b2-store:a-riverhog-b2-store-tcp-keepalive:8898b7b12a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-b2-store](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-1513c088ba"></a>

| Field | Value |
|---|---|
| <a id="s-9d88b5d281"></a>`consumers` | `["a-riverhog-b2-store"]` |
| <a id="s-bb088213c3"></a>`default_expressions` | `["''"]` |
| <a id="s-eeebfb92ac"></a>`id` | `"a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_TCP_KEEPALIVE"` |
| <a id="s-f5438b1f3d"></a>`input_shape` | `"environment-string"` |
| <a id="s-66c149272c"></a>`name` | `"A_RIVERHOG_B2_STORE_TCP_KEEPALIVE"` |
| <a id="s-f9c1542d89"></a>`owner` | `"a-riverhog-b2-store"` |

## Governing policies

- <a id="pa-7d6da6c2f3"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-b2-store:A_RIVERHOG_B2_STORE_TCP_KEEPALIVE](../../../evidence/sources/authorities.md#src-a61537cd22) — [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py::\_optional](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-b2-store` | [some-implementations/riverhog/storage/backblaze/src/a\_riverhog\_b2\_store/app.py](../../../../../../some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py) | `os.getenv(f'{_PREFIX}{name}', '')` |

### Machine authority

- `/external_contract/configuration_environment/93`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df7c344f1c2815820b5bc228ad59d67d8bfe1419f1e915d30202067654cf8ec2 -->

```json
{
  "consumers": [
    "a-riverhog-b2-store"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-b2-store:environment:A_RIVERHOG_B2_STORE_TCP_KEEPALIVE",
  "input_shape": "environment-string",
  "name": "A_RIVERHOG_B2_STORE_TCP_KEEPALIVE",
  "owner": "a-riverhog-b2-store"
}
```

</details>
