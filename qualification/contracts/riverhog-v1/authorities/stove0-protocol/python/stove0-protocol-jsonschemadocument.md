# stove0_protocol.JsonSchemaDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-jsonschemadocument:ded3a05798 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-611cd3c060"></a>
| Field | Shape |
|---|---|
| <a id="s-a29ecc0267"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-c16b8bf0fc"></a>`distribution` | "stove0-protocol" |
| <a id="s-848e1ded2e"></a>`module` | "stove0_protocol" |
| <a id="s-9babd6dbb9"></a>`name` | "JsonSchemaDocument" |
| <a id="s-2c06ac3bac"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_protocol.JsonSchemaDocument.from_schema](stove0-protocol-jsonschemadocument-from-schema.md)
- [stove0_protocol.JsonSchemaDocument.verify_digest](stove0-protocol-jsonschemadocument-verify-digest.md)

## Governing policies

- <a id="pa-4372580bc3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.JsonSchemaDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9cae64a24d2f7934840acc769da2fb81bcd602389ee08cfdce06f49d50c23002 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "044aac56d1ab78dca2a949caf25694292222a652e8b22484284cfd16c793e05f",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JsonSchemaDocument",
  "unit": "export"
}
```
