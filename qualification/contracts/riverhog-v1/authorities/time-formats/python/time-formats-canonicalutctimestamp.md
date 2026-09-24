# time_formats.CanonicalUtcTimestamp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:time-formats:time-formats-canonicalutctimestamp:fcf65563bd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [time-formats](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-01d8f6ddf4"></a>
- <a id="s-a3556db211"></a>`distribution`: `time-formats`
- <a id="s-f9c1f90d6c"></a>`module`: `time_formats`
- <a id="s-82b06b488f"></a>`name`: `CanonicalUtcTimestamp`
- <a id="s-6b634eba79"></a>`unit`: `export`

### Declared structure

- <a id="s-ad6538d9da"></a>`kind`: `"object"`
- <a id="s-a24d5f7b2c"></a>`type`: `"typing._AnnotatedAlias"`

## Governing policies

- <a id="pa-93fd17584e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:time-formats:time_formats](../../../evidence/sources/authorities.md#src-4f4a8234ef) — [packages/time-formats/src/time\_formats/\_\_init\_\_.py](../../../../../../packages/time-formats/src/time_formats/__init__.py)

### Machine authority

- `/external_contract/python/time_formats.CanonicalUtcTimestamp`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 07f927e0c6d06ed0d815443976308ec36ba6dbbf814c827b9771c694ba547e97 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._AnnotatedAlias"
  },
  "distribution": "time-formats",
  "module": "time_formats",
  "name": "CanonicalUtcTimestamp",
  "unit": "export"
}
```

</details>
