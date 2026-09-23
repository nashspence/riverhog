# stove0_operator_contracts.JoinAdmittedEventData

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-joinadmittedeventdata:41a71f9255 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34bc53e1a5"></a>
- <a id="s-7bc18ce55a"></a>`distribution`: `stove0-operator-contracts`
- <a id="s-f91b57dfc8"></a>`module`: `stove0_operator_contracts`
- <a id="s-8773b7f30d"></a>`name`: `JoinAdmittedEventData`
- <a id="s-134471b925"></a>`unit`: `export`

### Declared structure

- <a id="s-e9c2188c51"></a>`kind`: `"class"`
- <a id="s-39bebc050e"></a>`signature`: `"\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""`

#### Validated model schema

<a id="s-c37b1ced56"></a>

- <a id="s-0d04820f3d"></a>`type`: `"object"`
- <a id="s-3fad828fdd"></a>`additionalProperties`: `false`
- <a id="s-71b58caac1"></a>`required`: `["work_id","phase","revision","branch_set_sha256","join_plan_sha256","join_work_id"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e0637b5e57"></a>`branch_set_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-e742e6bb02"></a>`join_plan_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-56380e01ca"></a>`join_work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d657c3cda7"></a>`phase` | yes | type="string"; const="coordinating" |  |
| <a id="s-0dfa9b88a8"></a>`revision` | yes | type="integer"; minimum=2 |  |
| <a id="s-2de80ca85e"></a>`work_id` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

## Maintained corroboration

### Related interface records

- [__getitem__](stove0-operator-contracts-joinadmittedeventdata-getitem.md)
- [get](stove0-operator-contracts-joinadmittedeventdata-get.md)

## Governing policies

- <a id="pa-c1c198151d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources/authorities.md#src-51ad84528d) — [some-implementations/stove0/packages/operator-contracts/src/stove0\_operator\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_operator_contracts.JoinAdmittedEventData`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3667ed64e93ac4a26ddc5c99e4bc5214ed080d16d22059ebed02ee3f38408791 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "branch_set_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "join_plan_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "join_work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "phase": {
          "const": "coordinating",
          "type": "string"
        },
        "revision": {
          "minimum": 2,
          "type": "integer"
        },
        "work_id": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        }
      },
      "required": [
        "work_id",
        "phase",
        "revision",
        "branch_set_sha256",
        "join_plan_sha256",
        "join_work_id"
      ],
      "type": "object"
    },
    "signature": "\"(*, work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], phase: Literal['coordinating'], revision: Annotated[int, Ge(ge=2)], branch_set_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_plan_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], join_work_id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)]) -> None\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "JoinAdmittedEventData",
  "unit": "export"
}
```

</details>
