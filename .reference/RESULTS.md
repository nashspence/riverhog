# Numeric evaluation: source and finite evidence

This initial freeze contains the executable/test proposal and the frozen focused-verification driver. The separately frozen comparison script will be retained with its original execution receipts. No numeric performance result or clean-source verification is claimed before the retained comparison and focused checks execute.

The audited integration base is `824383a8a8143aac033f86dd1562a5979f6ec4da`. The owning request explicitly supplies the accepted reader and lookup tests from `0e9bef9445213aadadb7218fc6e268cc324f35f3` over that base. The numeric delta is measured against that accepted reader, not against the pre-lookup reader at the integration base. [The initial source audit](evidence/initial-source-audit.json) records this relationship and equal repository guidance/tool pins.

The final evidence append will identify the exact clean tested implementation separately from its published evidence commit. It will preserve original diagnostic and unmodified observations, public-behavior comparisons, focused test/controller/lane records, and cleanup. A later published integration checkpoint will be audited for relevant drift before final handoff; evidence from another source will not be relabeled as validation of that checkpoint.
