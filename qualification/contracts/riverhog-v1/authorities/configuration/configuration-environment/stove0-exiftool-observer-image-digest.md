# STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-observer-image-digest:3426b2a0c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3a00485a0d"></a>
| Field | Shape |
|---|---|
| <a id="s-f4162f50d7"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-811edc1b84"></a>`name` | "STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST" |

## Governing policies

- <a id="pa-854b07b1d8"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb461)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f50)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST](../../../evidence/sources.md#src-37e81b0b87) — `configuration-environment:STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/89`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de08938424adcaeb4ee10127c8a70b510942dd12d7fdd647363179c8afe2ea60 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_OBSERVER_IMAGE_DIGEST"
}
```
