# PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-upload-finalize-poll-seconds:2b47f46a96 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [configuration](../index.md) |
| Interface | [configuration-environment](index.md) |
| Family | [variables](families/variables/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-2436bad7d435"></a>
| Field | Shape |
|---|---|
| <a id="s-478f37a8400c"></a>`consumers` | ["piggity"] |
| <a id="s-7d2e712aed69"></a>`name` | "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS" |

### Progression, limits, and lifecycle

#### [extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

Shared facts for every subject below: configuration="PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"; maximum=null; reason="operator-configured-capacity"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](#s-2436bad7d435) | `value · configured-value · operational_policy` | shared above |

## Governing policies

- <a id="pa-614e67b697ad"></a>[compatibility/configuration/v1](../../../policies/index.md#p-8dc08bb46173)
- <a id="pa-8ecaa11cb682"></a>[extent-rule/configured-capacity/v1](../../../policies/index.md#p-3ebc61fc9972)

## Evidence

### Qualification

- [make unit](../../../evidence/sources.md#q-ce47068f504c)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS](../../../evidence/sources.md#src-2d353fa23bb2) — `configuration-environment:PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/configuration_environment/7`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8fe9cc66b3ddf76f9a71f4d2f1af74152b2d746b6188cdd977d31202fd1f86b -->

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_UPLOAD_FINALIZE_POLL_SECONDS"
}
```
