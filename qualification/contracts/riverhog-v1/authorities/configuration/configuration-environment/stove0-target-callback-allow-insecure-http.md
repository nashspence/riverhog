# STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-target-callback-allow-insecure-http:863ce92729 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-bd9bd4c4284d"></a>
| Field | Shape |
|---|---|
| <a id="s-46dd00a5c800"></a>`consumers` | ["stove0-server"] |
| <a id="s-2f10d7712341"></a>`name` | "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP" |

## Governing policies

- <a id="pa-903b301abdd8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP](../../../evidence/sources.md#src-ef417bf18193) — `configuration-environment:STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/113`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 048d152730400dbab3affefd03b86e4bc657e632918812a95132a5fff10e49b5 -->

```json
{
  "consumers": [
    "stove0-server"
  ],
  "name": "STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP"
}
```
