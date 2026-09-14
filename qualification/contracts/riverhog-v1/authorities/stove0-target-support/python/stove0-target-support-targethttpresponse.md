# stove0_target_support.TargetHttpResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targethttpresponse:fe302acd5c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-009ee483b0"></a>
- <a id="s-ee42d5b0af"></a>`distribution`: `stove0-target-support`
- <a id="s-7e9144e46d"></a>`module`: `stove0_target_support`
- <a id="s-1cf57c0b3d"></a>`name`: `TargetHttpResponse`
- <a id="s-52181a1049"></a>`unit`: `export`

### Declared structure

- <a id="s-ac8a5406b8"></a>`kind`: `"class"`
- <a id="s-24ddad11be"></a>`signature`: `"\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-b3abe12a6c"></a>`status` | `'int'` | `required` |
| <a id="s-fffd07c70b"></a>`headers` | `'tuple[tuple[str, str], ...]'` | `required` |
| <a id="s-82d750ca7c"></a>`body` | `'bytes'` | `required` |

## Governing policies

- <a id="pa-b6625d4e1c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.TargetHttpResponse`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8305da492e8ef942c9ae4819d68a5610e914ec6f02a63332c1957308b23ae359 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "status",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "headers",
        "type": "'tuple[tuple[str, str], ...]'"
      },
      {
        "default": "required",
        "name": "body",
        "type": "'bytes'"
      }
    ],
    "kind": "class",
    "signature": "\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "TargetHttpResponse",
  "unit": "export"
}
```
