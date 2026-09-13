# stove0_core.WorkFailure

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workfailure:1e4fa9d338 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1df2f7325c"></a>
| Field | Shape |
|---|---|
| <a id="s-1c9ba5f607"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2e88b5d7a5"></a>`distribution` | "stove0-server" |
| <a id="s-a52d535c83"></a>`module` | "stove0_core" |
| <a id="s-a9ee4ec0d2"></a>`name` | "WorkFailure" |
| <a id="s-5b867c9f0e"></a>`unit` | "export" |

## Governing policies

- <a id="pa-7c57ec297a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.WorkFailure`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7980135649a2dfa85c3b9f667e3ffc5eaa74328ea73a8af6a69e560c8220415c -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "1a48038aecad573b33ff882ab5b826265a854efe9f115ab1e115d1864ec2c4d0",
    "signature": "'(*, code: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool) -> None'"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkFailure",
  "unit": "export"
}
```
