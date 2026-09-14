# riverhog_provenance.LargeValueDisposition

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-provenance:riverhog-provenance-largevaluedisposition:cf8104c45b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6286966b5a"></a>
- <a id="s-5913808b38"></a>`distribution`: `riverhog-provenance`
- <a id="s-cf185b20a8"></a>`module`: `riverhog_provenance`
- <a id="s-7a9b3385cf"></a>`name`: `LargeValueDisposition`
- <a id="s-fdb441ed80"></a>`unit`: `export`

### Declared structure

- <a id="s-95f0e50c15"></a>`kind`: `"class"`
- <a id="s-60e3eee41c"></a>`signature`: `"'(*values)'"`

#### Enum members

| Member | Value |
|---|---|
| <a id="s-df86f32025"></a>`DIGEST_ONLY` | `"digest_only"` |
| <a id="s-32ce385652"></a>`FAIL` | `"fail"` |
| <a id="s-8db9a4023b"></a>`NOT_RETAINED` | `"not_retained"` |

## Maintained corroboration

### Related interface records

- [__format__](riverhog-provenance-largevaluedisposition-format.md)
- [__str__](riverhog-provenance-largevaluedisposition-str.md)

## Governing policies

- <a id="pa-9dd22814c3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-provenance:riverhog_provenance](../../../evidence/sources.md#src-38ef3a6054) — `packages/riverhog-provenance/src/riverhog_provenance/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_provenance.LargeValueDisposition`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 041260f5a464c98850561f68e9d2d22b259de9f4a367f8e6157e5dc2f08c370e -->

```json
{
  "contract": {
    "enum_values": {
      "DIGEST_ONLY": "digest_only",
      "FAIL": "fail",
      "NOT_RETAINED": "not_retained"
    },
    "kind": "class",
    "signature": "'(*values)'"
  },
  "distribution": "riverhog-provenance",
  "module": "riverhog_provenance",
  "name": "LargeValueDisposition",
  "unit": "export"
}
```
