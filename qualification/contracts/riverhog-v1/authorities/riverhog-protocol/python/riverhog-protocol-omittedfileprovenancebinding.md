# riverhog_protocol.OmittedFileProvenanceBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-omittedfileprovenancebinding:ec3d5472bb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-76a121b9ba"></a>
| Field | Shape |
|---|---|
| <a id="s-1c07cec3c4"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-2c93b17ba5"></a>`distribution` | "riverhog-protocol" |
| <a id="s-5dd170f00b"></a>`module` | "riverhog_protocol" |
| <a id="s-45c2cabfb1"></a>`name` | "OmittedFileProvenanceBinding" |
| <a id="s-d2a0760302"></a>`unit` | "export" |

## Governing policies

- <a id="pa-9f8ee361ad"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.OmittedFileProvenanceBinding`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5872ce0b930426e8429aefc994e689232b6c913b424ece795f0dac801a083a62 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "5eba6adbe60f961048cf4ff31b645ca43fb22f9f4ff95b3476666b75a05b8a3c",
    "signature": "\"(*, status: Literal['omitted'], omission_reason: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\\\\\S(?:[\\\\\\\\s\\\\\\\\S]*\\\\\\\\S)?$', ascii_only=None)]) -> None\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "OmittedFileProvenanceBinding",
  "unit": "export"
}
```
