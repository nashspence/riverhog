# Research and corrected assumptions for #959

Checked 2026-10-05. Primary-source observations are distinct from the recommendations
in the owning issue. Recheck volatile pricing, engine support, authentication and
product capabilities when this post-v1 work is actually implemented.

## NiCad and the evidentiary ceiling

[NiCad README, exact source](https://github.com/CordyJ/Open-NiCad/blob/7a90d11795a7fe585282e25b7fa8d9964f202965/README.txt)
identifies version 7.0, Python support, OpenTxl 11+ and a build step. NiCadCross
compares two supplied systems; it is not an internet-wide discovery engine.
Thresholds concern different pretty-printed lines. Default profiles use blind
renaming and 0.30; the recommendation explicitly does not inherit that default.
The [Type-2 profile](https://github.com/CordyJ/Open-NiCad/blob/7a90d11795a7fe585282e25b7fa8d9964f202965/src/config/type2-report.cfg)
uses threshold 0, blind renaming and explicit fragment-size limits. The repository
includes a permissive [license](https://github.com/CordyJ/Open-NiCad/blob/7a90d11795a7fe585282e25b7fa8d9964f202965/LICENSE.txt).
The inspected source is a research pin, not a tested production build pin.

Consequences: qualify real Riverhog syntax; use raw originals and exact ranges;
record extraction omissions. Rename-normalized matches are useful leads, not proof
of copying, ownership or a breached license. A renamed project that no discovery
channel finds never reaches NiCad. No proprietary detector or custom semantic/ML
fingerprint service is necessary for the scoped first implementation.

[GitHub's DMCA guide](https://docs.github.com/en/site-policy/content-removal-policies/guide-to-submitting-a-dmca-takedown-notice)
requires specific identification and careful investigation, distinguishes expression
from functionality and permissible license use, and warns against automated bulk
complaints. It is a platform procedure, not a court evidentiary rule. No source
reviewed establishes NiCad as court-approved or guarantees admissibility. The
recommended output is reproducible source evidence for qualified human review,
not an automatic accusation based on a score.

## Aurora is plausible, but not free by definition

[Aurora automatic pause/resume](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2-auto-pause.html)
allows compatible engine versions to pause at minimum 0 ACUs. Capacity charges stop
while paused, not storage/other charges. Open user connections and RDS Proxy can
prevent pausing; maintenance wakes instances. Long idle periods can have longer
resume delays. Do not keep a connection pool or SQL heartbeat alive accidentally.

[Scaling configuration](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_ServerlessV2ScalingConfiguration.html)
allows an idle-pause interval starting at 300 seconds. [DatabaseResumingException](https://docs.aws.amazon.com/botocore/latest/reference/services/rds-data/client/exceptions/DatabaseResumingException.html)
means a Data API request can resume the database but require a retry. Record real
engine/region and cold-start qualification, not an assumed immediate wake-up.

[Aurora pricing](https://aws.amazon.com/rds/aurora/pricing/) separates compute,
storage/I/O and optional features. [Storage scaling documentation](https://docs.aws.amazon.com/rds/latest/auroraextendedcontent/aurora-faq-scalability.html)
describes a normal 10 GiB starting allocation; current new-account offers on the
pricing page have different limited allowances. Resolve the actual regional/account
terms in the deployment estimate. [Secrets Manager](https://aws.amazon.com/secrets-manager/pricing/)
has its own charges. Include backups, key custody, managed service/logging/alerts,
maintenance and a failed-to-pause scenario. No specific cheap monthly total has
been established here, and no spending is authorized by this reference.

## Data API is not an unrestricted PostgreSQL socket

[Access authorization](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.access.html)
requires IAM permission and Secrets Manager database credentials. OIDC removes
long-lived AWS access keys from GitHub, not the underlying database passwords or
key-custody responsibility. Scope resources/secrets and SQL roles independently.

[Data API troubleshooting](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.troubleshooting.html)
documents a 64 KB returned-row limit, 1 MiB result limit, a three-minute transaction
idle timeout, and unsupported multi-statements. [BatchExecuteStatement](https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_BatchExecuteStatement.html)
limits the entire request to 4 MiB, including representation overhead. A large
`bytea` value is not automatically retrievable through this transport. The specimen
uses 16 KiB chunks and eight-row batches; actual AWS wrappers must also be measured.
No transaction should span source acquisition, NiCad or remote verification.

Use ordinary PostgreSQL tables and a narrow adapter. Implement real uniqueness,
concurrent/idempotent finalization and role tests. Plain portable SQL does not make
Data API support `pg_dump`; supply an application export/import or explicit temporary
managed network access for native tools. Neither requires a personal host.

## Workflow trust and transfer

[GitHub AWS OIDC guidance](https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws)
requires constrained subject/audience trust. Environment-bound subjects differ from
branch subjects. [OIDC reference](https://docs.github.com/en/actions/reference/security/oidc)
now documents immutable owner/repository ID formats and changes on repository
creation/rename/transfer. Validate the actual issued subject rather than copying
an old name-only string. Do not expose the complete token in diagnostic output.

[Artifact attestation guidance](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
supports provenance for public repositories with OIDC signing. [Verifier options](https://cli.github.com/manual/gh_attestation_verify)
allow expected workflow/source constraints. Preserve verification material, exact
ciphertext and independent run/attempt binding. A signature or a hash manifest is
not proof of an infringement, a true observation, or immutable future retention.

[Scheduled workflow limitations](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
include delayed/dropped runs and inactivity disabling in public repositories.
Freshness cannot be guaranteed by the same possibly disabled workflow.
[Artifact retention](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/download-workflow-artifacts)
is a short-term transfer mechanism, not the durable evidence authority. Public
artifact readers must see ciphertext only; expirations do not erase prior downloads.

The recommended separation of analysis and publisher jobs prevents candidate
parsers from sharing the cloud-writer execution context. Encryption alone cannot
produce searchable SQL fields. A managed importer verifies/decrypts then indexes;
its identity belongs in managed secret custody, not on the original owner's machine.
These are design consequences, not features magically supplied by Aurora.

## Managed review boundary

[MCP security guidance](https://modelcontextprotocol.io/docs/2026-07-28/tutorials/security/security_best_practices)
covers authorization, token passthrough and unsafe fetch boundaries. The proposed
read interface accepts opaque IDs/cursors, not arbitrary URLs/paths/SQL. Read-only
annotations do not enforce access control or neutralize prompt injection.
Selected plaintext returned to a model is disclosure to that model service.

[ChatGPT scheduled tasks](https://help.openai.com/en/articles/10291617-scheduled-tasks-in-chatgpt)
describes account/app-dependent support and connected apps; it is not proof that
this unbuilt custom MCP/OAuth deployment will run unattended. Verify that separately.
Cloud custody/ingestion/freshness and ordinary authenticated export must work without
ChatGPT, a chat history, an interactive session or a private home server.

## Remaining checks before any deployment

Actual NiCad/OpenTxl build and parser behavior; age/tamper tests; AWS regional cost
and pause measurements; real Data API limits/retries/SQL grants; signed workflow
provenance; managed importer authentication; backups and fresh-account restoration;
secret transfer/revocation; and actual MCP/account capabilities remain untested.
The twelve pure specimen tests do not substitute for those checks.
