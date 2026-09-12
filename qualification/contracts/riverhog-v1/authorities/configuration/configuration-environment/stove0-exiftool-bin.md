# STOVE0_EXIFTOOL_BIN

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:stove0-exiftool-bin:41bb0dad1c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-41e02220a1e1"></a>
| Field | Shape |
|---|---|
| <a id="s-84903db1b5a0"></a>`consumers` | ["stove0-exiftool-observer"] |
| <a id="s-f3010b390a89"></a>`name` | "STOVE0_EXIFTOOL_BIN" |

## Governing policies

- <a id="pa-5447cc01de92"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:STOVE0_EXIFTOOL_BIN](../../../evidence/sources.md#src-35bf242d8aa2) — `configuration-environment:STOVE0_EXIFTOOL_BIN`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/87`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 80ab2cd142180343f0f7f4e80a0ddb1b726ae96c440a7a95fdd1673a8d71c9a3 -->

```json
{
  "consumers": [
    "stove0-exiftool-observer"
  ],
  "name": "STOVE0_EXIFTOOL_BIN"
}
```
