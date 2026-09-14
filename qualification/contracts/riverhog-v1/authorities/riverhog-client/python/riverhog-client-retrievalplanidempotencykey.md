# riverhog_client.RetrievalPlanIdempotencyKey

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-retrievalplanidempotencykey:6e33924706 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0eb6aeb3c1"></a>
- <a id="s-a30d1da8f2"></a>`distribution`: `riverhog-client`
- <a id="s-3f5ca47f2b"></a>`module`: `riverhog_client`
- <a id="s-092cd5dbdc"></a>`name`: `RetrievalPlanIdempotencyKey`
- <a id="s-05bc4b6d4f"></a>`unit`: `export`

### Declared structure

- <a id="s-acf92c40f6"></a>`kind`: `"type-alias"`
- <a id="s-3aaf6e37c2"></a>`value`: `"typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), FieldInfo(annotation=NoneType, required=True, metadata=[MaxLen(max_length=200)])]"`

## Governing policies

- <a id="pa-08f2ae29fd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.RetrievalPlanIdempotencyKey`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3104f224af196ea21d6200b18ad368dc61424aebdc388d8426b484e48d56b375 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=None, pattern='^\\\\S(?:[\\\\s\\\\S]*\\\\S)?$', ascii_only=None), FieldInfo(annotation=NoneType, required=True, metadata=[MaxLen(max_length=200)])]"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "RetrievalPlanIdempotencyKey",
  "unit": "export"
}
```
