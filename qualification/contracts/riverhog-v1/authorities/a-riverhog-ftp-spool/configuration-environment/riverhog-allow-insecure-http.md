# RIVERHOG_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-ftp-spool:riverhog-allow-insecure-http:094f28d2c9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-a5bdad2233"></a>

| Field | Value |
|---|---|
| <a id="s-ee93841a02"></a>`consumers` | `["a-riverhog-ftp-spool"]` |
| <a id="s-27341a9850"></a>`default_expressions` | `["'false'"]` |
| <a id="s-1eb66f737d"></a>`id` | `"a-riverhog-ftp-spool:environment:RIVERHOG_ALLOW_INSECURE_HTTP"` |
| <a id="s-6e5299b395"></a>`input_shape` | `"environment-string"` |
| <a id="s-4ca352f816"></a>`name` | `"RIVERHOG_ALLOW_INSECURE_HTTP"` |
| <a id="s-2bd29e5838"></a>`owner` | `"a-riverhog-ftp-spool"` |

## Governing policies

- <a id="pa-2b397fc7c5"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-ftp-spool:RIVERHOG_ALLOW_INSECURE_HTTP](../../../evidence/sources/authorities.md#src-53821eb58c) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py::load\_config](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-ftp-spool` | [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py) | `os.environ.get('RIVERHOG_ALLOW_INSECURE_HTTP', 'false')` |

### Machine authority

- `/external_contract/configuration_environment/112`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f2dd562bf87282a1f92478dc38aa1d885c25f442ef04d6c16d31708c62fba9b -->

```json
{
  "consumers": [
    "a-riverhog-ftp-spool"
  ],
  "default_expressions": [
    "'false'"
  ],
  "id": "a-riverhog-ftp-spool:environment:RIVERHOG_ALLOW_INSECURE_HTTP",
  "input_shape": "environment-string",
  "name": "RIVERHOG_ALLOW_INSECURE_HTTP",
  "owner": "a-riverhog-ftp-spool"
}
```

</details>
