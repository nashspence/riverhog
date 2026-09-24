# stove0_operator_contracts.Stove0Event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0event:bdea015bfe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c9062d92d3"></a>
- <a id="s-06b55a96d4"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-e60bf8a08e"></a>`module`: `stove0_operator_contracts`
- <a id="s-00c34335a7"></a>`name`: `Stove0Event`
- <a id="s-da036943ef"></a>`unit`: `export`

### Declared structure

- <a id="s-04e175a556"></a>`kind`: `"class"`
- <a id="s-6c8ccc0c3a"></a>`signature`: `"\"(*, id: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str, MinLen(min_length=1)], occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: Any) -> None\""`

#### Validated model schema

<a id="s-ad1a7a15b8"></a>

- <a id="s-5c601d80f4"></a>`type`: `"object"`
- <a id="s-1f491d4d4e"></a>`additionalProperties`: `false`
- <a id="s-e943b66aaf"></a>`required`: `["id","type","subject","occurred_at","payload"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c61b59b0fb"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-a08d3bc18b"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-a1b0930b40"></a>`payload` | yes | any JSON value |  |
| <a id="s-df434406b7"></a>`subject` | yes | type="string"; minLength=1 |  |
| <a id="s-9a6e492224"></a>`type` | yes | type="string"; minLength=1 |  |

## Maintained corroboration

### Related interface records

- [exact_subject](stove0-operator-contracts-stove0event-exact-subject.md)

## Governing policies

- <a id="pa-dab5c2c669"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.Stove0Event`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4306952a2779f10d726eba67402511d7268e6a9a95ed4aab5963b34c03b23611 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "minLength": 1,
          "type": "string"
        },
        "occurred_at": {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        "payload": {},
        "subject": {
          "minLength": 1,
          "type": "string"
        },
        "type": {
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "id",
        "type",
        "subject",
        "occurred_at",
        "payload"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, MinLen(min_length=1)], type: Annotated[str, MinLen(min_length=1)], subject: Annotated[str, MinLen(min_length=1)], occurred_at: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=30, max_length=30, pattern='^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\\\\\\\.[0-9]{9}Z$', ascii_only=None), AfterValidator(func=<function require_canonical_utc_timestamp>)], payload: Any) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "Stove0Event",
  "unit": "export"
}
```

</details>
