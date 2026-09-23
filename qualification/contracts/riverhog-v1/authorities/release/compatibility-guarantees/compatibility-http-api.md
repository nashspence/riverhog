# Compatibility: http api

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: compatibility-guarantees:release:compatibility-http-api:bd82c796ba -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Compatibility Guarantees](index.md) |

## External contract

<a id="p-5bc717c2c0"></a>
[Indexed applications](../../../policies/compatibility-http-api-v1/applications.md)


| Field | Value |
|---|---|
| <a id="s-9e2eb4b18c"></a>`http_api` | `"Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."` |

## Evidence

### Qualification

- [make release-check](../../../evidence/sources/commands.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [release:release.toml](../../../evidence/sources/authorities.md#src-c5380dbe5f) — [release.toml](../../../../../../release.toml)

### Machine authority

- `/external_contract/release/compatibility/http_api`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2585cfdddab6eb45045fb1e0a19776f07029a40553fc17b28811f710586512ac -->

```json
"Published v1 HTTP and CloudEvents contracts remain backward compatible throughout v1."
```

</details>
