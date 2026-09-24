# schemas: AppKeyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-appkeyout:8415bf5538 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-30b79f4da3"></a>

- <a id="s-78e9e2b54f"></a>`type`: `"object"`
- <a id="s-042bdf4019"></a>`additionalProperties`: `false`
- <a id="s-caa13a1a82"></a>`required`: `["id","app","access","monthly_download_quota_bytes","status","created_at","expires_at","revoked_at","last_used_at"]`
- <a id="s-b5bbd41ed4"></a>`title`: `"AppKeyOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de20ad219d"></a>`access` | yes | [ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md) |  |
| <a id="s-f98913f6b5"></a>`app` | yes | [ApplicationName](schemas-applicationname.md) |  |
| <a id="s-a7a363cae1"></a>`created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"; title="Created At" |  |
| <a id="s-f354ddeeb2"></a>`expires_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Expires At" |  |
| <a id="s-f8ace5c134"></a>`id` | yes | [ApplicationKeyId](schemas-applicationkeyid.md) |  |
| <a id="s-ff83a181db"></a>`last_used_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Last Used At" |  |
| <a id="s-3607996617"></a>`monthly_download_quota_bytes` | yes | anyOf=[([MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)); (type="null")] |  |
| <a id="s-7354f9aecf"></a>`revoked_at` | yes | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; title="Revoked At" |  |
| <a id="s-d6933d5f36"></a>`status` | yes | type="string"; enum=["active","expired","revoked"]; title="Status" |  |

### All must match (`allOf`)

| Rule | If schema matches | Then must match | Otherwise must match |
|---|---|---|---|
| <a id="s-95865a7804"></a>1 | properties={status: (const="revoked")} | properties={revoked_at: (type="string")} | properties={revoked_at: (type="null")} |
| <a id="s-24e267f0a5"></a>2 | properties={status: (const="expired")} | properties={expires_at: (type="string")} | no additional constraint |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=30; minimum=30; reason="fixed-public-representation"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field created_at](#s-a7a363cae1) | `length · characters · fixed` | shared above |
| <a id="s-331bf5aca5"></a>[field expires_at · string value](#s-f354ddeeb2) | `length · characters · fixed` | shared above |
| <a id="s-50cc40c5f5"></a>[field last_used_at · string value](#s-ff83a181db) | `length · characters · fixed` | shared above |
| <a id="s-bc55cd8474"></a>[field revoked_at · string value](#s-7354f9aecf) | `length · characters · fixed` | shared above |

## Maintained corroboration

### Referenced contract elements

- [ApplicationAccessGrantSet](schemas-applicationaccessgrantset.md)
- [ApplicationKeyId](schemas-applicationkeyid.md)
- [ApplicationName](schemas-applicationname.md)
- [MonthlyDownloadQuotaBytes](schemas-monthlydownloadquotabytes.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-295df1942f"></a>[compatibility/http-api/v1](../../release/compatibility-guarantees/compatibility-http-api.md#p-5bc717c2c0)
- <a id="pa-9900be29a7"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources/commands.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources/authorities.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/AppKeyOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: df918ef9c45cfbfb3148aeabd3e66c3cca727476ae69e4ff3002a3b5b44eb3b9 -->

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
    "last_used_at"
  ],
  "title": "AppKeyOut",
  "type": "object"
}
```

</details>
