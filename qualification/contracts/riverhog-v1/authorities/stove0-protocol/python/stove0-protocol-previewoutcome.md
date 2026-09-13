# stove0_protocol.PreviewOutcome

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-previewoutcome:f3205c851d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9f2bbb4146"></a>
| Field | Shape |
|---|---|
| <a id="s-8db03f362d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-77cec49fc7"></a>`distribution` | "stove0-protocol" |
| <a id="s-97b6b20862"></a>`module` | "stove0_protocol" |
| <a id="s-789d60d6d9"></a>`name` | "PreviewOutcome" |
| <a id="s-f453d1dc75"></a>`unit` | "export" |

## Governing policies

- <a id="pa-10bdecf1bf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.PreviewOutcome`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 06672add356b71346faf4214f1c158e4c18ea9d89eb39761ce6091cb9b97ae0f -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "83bbb579a08b1ff84c5c4771b7f20a911c71ba952de33cb6bc91c9db92b4f632",
    "signature": "\"(*, code: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], message: Annotated[str, MinLen(min_length=1), MaxLen(max_length=1000)], retryable: bool | None = None) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "PreviewOutcome",
  "unit": "export"
}
```
