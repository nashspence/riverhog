# schemas: AppKeyCreatedOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-appkeycreatedout:7fcc88a6a1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-cd113a1d81"></a>

- <a id="s-3b25d9574e"></a>`type`: `"object"`
- <a id="s-d864794007"></a>`additionalProperties`: `false`
- <a id="s-ffc8b29a5b"></a>`required`: `["id","app","access","monthly_download_quota_bytes","status","created_at","expires_at","revoked_at","last_used_at","token"]`
- <a id="s-d5abee0256"></a>`title`: `"AppKeyCreatedOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-586f65365e"></a>`access` | yes | [ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md) |  |
| <a id="s-48bcb68302"></a>`app` | yes | [ApplicationName](schemas-applicationname.md) |  |
| <a id="s-7b641a4bb0"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Created At" |  |
| <a id="s-d9ca89d4ca"></a>`expires_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Expires At" |  |
| <a id="s-5ccd64df56"></a>`id` | yes | [ApplicationKeyId](schemas-applicationkeyid.md) |  |
| <a id="s-9dbd92c3f8"></a>`last_used_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Last Used At" |  |
| <a id="s-54bec0ed94"></a>`monthly_download_quota_bytes` | yes | anyOf=[([MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)); (type="null")] |  |
| <a id="s-e253c7e9c1"></a>`revoked_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Revoked At" |  |
| <a id="s-41bdd094b2"></a>`status` | yes | type="string"; enum=["active","expired","revoked"]; title="Status" |  |
| <a id="s-7def1d8177"></a>`token` | yes | type="string"; title="Token" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-10d16a3d18"></a>1 | properties={status: (const="revoked")} | properties={revoked_at: (type="string")} | properties={revoked_at: (type="null")} |
| <a id="s-f7ba579e11"></a>2 | properties={status: (const="expired")} | properties={expires_at: (type="string")} | no additional constraint |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field created_at](#s-7b641a4bb0) | `length · characters · fixed` | shared above |
| <a id="s-fffdcd9131"></a>[field expires_at · string value](#s-d9ca89d4ca) | `length · characters · fixed` | shared above |
| <a id="s-a72d32019e"></a>[field last_used_at · string value](#s-9dbd92c3f8) | `length · characters · fixed` | shared above |
| <a id="s-e6e292be21"></a>[field revoked_at · string value](#s-e253c7e9c1) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)
- [ApplicationKeyId](schemas-applicationkeyid.md)
- [ApplicationName](schemas-applicationname.md)
- [MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-04b352a05c"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-480f15dfd0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppKeyCreatedOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6aa1011e3b34f78e8adaa2b676b5b35034e43dd8ffcb5f8910537fa0f035040f -->

```json
{
  "additionalProperties": false,
  "allOf": [
    {
      "else": {
        "properties": {
          "revoked_at": {
            "type": "null"
          }
        }
      },
      "if": {
        "properties": {
          "status": {
            "const": "revoked"
          }
        }
      },
      "then": {
        "properties": {
          "revoked_at": {
            "type": "string"
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "status": {
            "const": "expired"
          }
        }
      },
      "then": {
        "properties": {
          "expires_at": {
            "type": "string"
          }
        }
      }
    }
  ],
  "properties": {
    "access": {
      "$ref": "#/components/schemas/ApplicationAccessGrantSet"
    },
    "app": {
      "$ref": "#/components/schemas/ApplicationName"
    },
    "created_at": {
      "maxLength": 30,
      "minLength": 30,
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
      "title": "Created At",
      "type": "string"
    },
    "expires_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Expires At"
    },
    "id": {
      "$ref": "#/components/schemas/ApplicationKeyId"
    },
    "last_used_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Last Used At"
    },
    "monthly_download_quota_bytes": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/MonthlyDownloadQuotaBytes"
        },
        {
          "type": "null"
        }
      ]
    },
    "revoked_at": {
      "anyOf": [
        {
          "maxLength": 30,
          "minLength": 30,
          "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Revoked At"
    },
    "status": {
      "enum": [
        "active",
        "expired",
        "revoked"
      ],
      "title": "Status",
      "type": "string"
    },
    "token": {
      "title": "Token",
      "type": "string"
    }
  },
  "required": [
    "id",
    "app",
    "access",
    "monthly_download_quota_bytes",
    "status",
    "created_at",
    "expires_at",
    "revoked_at",
    "last_used_at",
    "token"
  ],
  "title": "AppKeyCreatedOut",
  "type": "object"
}
```

</details>
