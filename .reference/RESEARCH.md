# Primary-source research, checked 2026-10-05

The issue and implementation deliberately replace the conversation's speculative
claims with the following narrower conclusions. No legal enforcement conclusion is
made by the tooling.

## GitHub discovery and scheduling

[REST search](https://docs.github.com/en/rest/search/search) documents default-branch
code indexing, file/index limits, up to 1,000 accessible search results, pagination,
`incomplete_results` and a 10-request/minute code-search limit. Its generic token
section lists fine-grained/App tokens and says public resources can be unauthenticated,
while the code-search section requires authentication. That inconsistency is why
activation must probe the actual chosen token rather than promise GITHUB_TOKEN
success or unauthenticated code search. The API response `sha` is a **blob** identity.
The reference does not reuse browser-code-search regex syntax or invent commit IDs.

[Fork enumeration](https://docs.github.com/en/rest/repos/forks) is a separate REST
inventory, not evidence that any particular licensed component remains in a fork.
Distinctive protocol strings may also identify interoperable independent code.
[Scheduled events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule)
run on the default branch, can be delayed/dropped, and public-repository schedules
can be disabled after 60 days without activity. This is not a reliable sole watchdog.
The reference uses an off-hour minute and requires an independent host freshness check.

## Artifacts, signatures and trust

[Artifacts API](https://docs.github.com/en/rest/actions/artifacts) describes artifact
IDs, expiration and redirect-based downloads. Public artifacts are not private
storage; short retention is neither durable archival nor guaranteed deletion from
other people's copies. Model/runtime access to public artifacts still depends on the
actual client/authentication path; the design does not equate 'public' with every
endpoint accepting anonymous download.

[Secure Actions guidance](https://docs.github.com/en/actions/reference/security/secure-use)
supports least privilege, full commit pins and isolating untrusted input. The
reference never executes matched downstream code and scopes the optional search
credential to collection. The signer still trusts the approved collector code.

[Artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
provide OIDC-backed provenance for public-repository artifacts without adding a
long-lived signing secret. The reference attests only ciphertext. The actual
[actions/attest action.yml](https://github.com/actions/attest/blob/1e69f48acb82d1966a394da916b4c1698aa569d6/action.yml)
was fetched through the GitHub connector; the v4 ref resolved to that exact commit.
It supports `subject-path`, `show-summary` and the `bundle-path` output.

[gh attestation verify](https://cli.github.com/manual/gh_attestation_verify) exposes
repository, signer-workflow, source-ref/source-digest and hosted-runner policies,
JSON verification results and offline bundles. Signature validity alone does not
approve the producing workflow. In particular, predicate contents are workflow-
controllable; OIDC certificate fields and witnessed timestamps have a different
trust basis. The host must independently bind the approved source and run attempt.

[age](https://age-encryption.org/) is the existing repo encryption tool. Encryption
restricts readers; it does not authenticate who produced the evidence. A plaintext
hash manifest also cannot prove origin by itself. The repository's `mise.lock`
pins age 1.3.1 and its platform artifacts; use that existing tool rather than a
parallel cryptographic implementation. The reference's native test remains
unperformed in this external environment, as recorded in HANDOFF.md.

## Private-host MCP and scheduled model access

[MCP security guidance](https://modelcontextprotocol.io/docs/2025-11-25/tutorials/security/security_best_practices)
covers authorization, token passthrough, SSRF and least-privilege boundaries.
A narrowly scoped read-only server is preferable to shell/decrypt(path)/fetch(url)
tools. Read-only annotations do not enforce authorization or prevent model prompt
injection; evidence still needs untrusted-content handling and output controls.

[ChatGPT Tasks](https://help.openai.com/en/articles/10291617-scheduled-tasks-in-chatgpt)
currently describes connected-app support.
[Custom MCP connectivity](https://help.openai.com/en/articles/12584461-developer-mode-and-mcp-apps-in-chatgpt)
describes remote-server connectivity and private/local connectivity options.
Neither is evidence that this unconfigured host/account's authentication and tools
will work unattended. That is an explicit deployment test, not a repository
qualification promise. No actual ChatGPT task, custom app, tunnel or host was
provisioned during this handoff.
