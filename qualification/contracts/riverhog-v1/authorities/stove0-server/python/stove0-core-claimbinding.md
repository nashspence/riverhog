# stove0_core.ClaimBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-claimbinding:82a31f5db2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b809db080b"></a>
| Field | Shape |
|---|---|
| <a id="s-d79efb3af0"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-94c1857fc6"></a>`distribution` | "stove0-server" |
| <a id="s-6c8880c007"></a>`module` | "stove0_core" |
| <a id="s-7936ede901"></a>`name` | "ClaimBinding" |
| <a id="s-222ae778d6"></a>`unit` | "export" |

## Governing policies

- <a id="pa-de4fba4083"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.ClaimBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 221a244115ee6980aa1c65fe5976da9ef67bfaf5916f9944512c5ead5e1d4348 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "659936b8015faa931dac1f67ac09090a862453b08c01b2b835ee6c759c8b2a58",
    "signature": "'(*, claim_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], fence: Annotated[int, Ge(ge=1)]) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "ClaimBinding",
  "unit": "export"
}
```
