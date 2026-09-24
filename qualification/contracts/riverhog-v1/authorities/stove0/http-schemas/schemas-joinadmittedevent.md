# schemas: JoinAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:stove0:schemas-joinadmittedevent:106712145f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-89c2b9e530"></a>

- <a id="s-797a81b125"></a>`type`: `"object"`
- <a id="s-e6f1ea9542"></a>`additionalProperties`: `false`
- <a id="s-d719486eb3"></a>`required`: `["id","source","type","subject","time","data"]`
- <a id="s-b5df77a5be"></a>`title`: `"JoinAdmittedEvent"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7dff2842c2"></a>`data` | yes | [JoinAdmittedEventData](schemas-joinadmittedeventdata.md) |  |
| <a id="s-2ce0d77104"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json"; title="Datacontenttype" |  |
| <a id="s-3adf064b80"></a>`id` | yes | type="string"; minLength=1; title="Id" |  |
| <a id="s-0800c849d6"></a>`source` | yes | type="string"; const="urn:riverhog:stove0"; title="Source" |  |
| <a id="s-443d205ec8"></a>`specversion` | no | type="string"; const="1.0"; default="1.0"; title="Specversion" |  |
| <a id="s-ae8b6da303"></a>`subject` | yes | type="string"; minLength=1; title="Subject" |  |
| <a id="s-6d92c96711"></a>`time` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Time" |  |
| <a id="s-dbf4280eb9"></a>`type` | yes | type="string"; const="io.riverhog.stove0.join.admitted"; title="Type" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field time](#s-6d92c96711) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [JoinAdmittedEventData](schemas-joinadmittedeventdata.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-eda5c5b206"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-ca39938afb"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:stove0](../../../evidence/sources/authorities.md#src-52e6e32124) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/JoinAdmittedEvent`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3931634ded7746bc582c6fbecfcd9b0ac7bc78ea4992fcd537a71e4019314a47 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/JoinAdmittedEventData"
    },
    "datacontenttype": {
      "const": "application/json",
      "default": "application/json",
      "title": "Datacontenttype",
      "type": "string"
    },
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "const": "urn:riverhog:stove0",
      "title": "Source",
      "type": "string"
    },
    "specversion": {
      "const": "1.0",
      "default": "1.0",
      "title": "Specversion",
      "type": "string"
    },
    "subject": {
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "time": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.join.admitted",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "subject",
    "time",
    "data"
  ],
  "title": "JoinAdmittedEvent",
  "type": "object"
}
```

</details>
