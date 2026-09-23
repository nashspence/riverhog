# RIVERHOG_BASE_URL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:a-riverhog-ftp-spool:riverhog-base-url:84c942aef2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [Configuration Environment](index.md) |

## External contract

<a id="s-bd9bd4c428"></a>

| Field | Value |
|---|---|
| <a id="s-46dd00a5c8"></a>`consumers` | `["a-riverhog-ftp-spool"]` |
| <a id="s-a39501b2d0"></a>`default_expressions` | `["''"]` |
| <a id="s-d7df8d8378"></a>`id` | `"a-riverhog-ftp-spool:environment:RIVERHOG_BASE_URL"` |
| <a id="s-409705dbe5"></a>`input_shape` | `"environment-string"` |
| <a id="s-2f10d77123"></a>`name` | `"RIVERHOG_BASE_URL"` |
| <a id="s-1bda924b26"></a>`owner` | `"a-riverhog-ftp-spool"` |

## Governing policies

- <a id="pa-48abe2cd72"></a>[compatibility/configuration/v1](../../release/compatibility-guarantees/compatibility-configuration.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources/commands.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [configuration-environment:a-riverhog-ftp-spool:RIVERHOG_BASE_URL](../../../evidence/sources/authorities.md#src-3f1093d4e2) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py::load\_config](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Configuration authority and bindings

The owning implementation defines the setting. These declarations, consumer bindings, and default expressions are discovered source facts, not executed observations of effective configuration.

| Kind | Consumer | Source | Authority |
|---|---|---|---|
| parser | `a-riverhog-ftp-spool` | [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/config.py](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/config.py) | `os.environ.get('RIVERHOG_BASE_URL', '')` |

### Machine authority

- `/external_contract/configuration_environment/113`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3bb7967a23518b54b3ab01df6aadca23533f8b9b199329c26ea0e8d64e2bdc6b -->

```json
{
  "consumers": [
    "a-riverhog-ftp-spool"
  ],
  "default_expressions": [
    "''"
  ],
  "id": "a-riverhog-ftp-spool:environment:RIVERHOG_BASE_URL",
  "input_shape": "environment-string",
  "name": "RIVERHOG_BASE_URL",
  "owner": "a-riverhog-ftp-spool"
}
```

</details>
