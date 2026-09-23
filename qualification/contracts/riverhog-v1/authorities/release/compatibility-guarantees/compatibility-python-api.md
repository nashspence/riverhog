# Compatibility: python api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-python-api:20c622516a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="p-e574772ba5"></a>
[Indexed applications](../../../policies/compatibility-python-api-v1/applications.md)


| Field | Value |
|---|---|
| <a id="s-7d6f9abc79"></a>`python_api` | `"Freeze-protected declared public-module exports, callable signatures, selected constants, enum members and values, and selected public model and dataclass structures remain backward compatible throughout v1."` |

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/compatibility/python_api`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9e36c233d9c32bdf67be4f55acc6a0fa07e262c5d5e73466ce68307d445bdf4f -->

```json
"Freeze-protected declared public-module exports, callable signatures, selected constants, enum members and values, and selected public model and dataclass structures remain backward compatible throughout v1."
```

</details>
