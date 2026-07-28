// scenarios/suite.js - RETIRED.
//
// This script hit a generic, unauthenticated /api/v1/payments endpoint with the caller supplying
// chargeId directly in the payload (no VA resolution, no checksum/auth, six bank codes three of
// which - BCA/BNI/BRI - have no adapter in the relational payment-gateway repo at all). It was not
// comparable to the sibling repo's scenarios/suite-rdbms.js, which drives the real production BSI
// adapter with checksum auth. See docs/benchmark-remediation-guideline.md findings F1, F5, F7 for
// the full audit.
//
// Use scenarios/suite-bsi.js instead: it hits this repo's real /api/bank/bsi adapter with the
// same request shape and checksum scheme as scenarios/suite-rdbms.js, so both systems are
// benchmarked through the identical BSI protocol. It is the ONLY evtsrc script used for the
// head-to-head comparison from now on.
//
// This file is kept (rather than deleted) only so that running it fails loudly with this message
// instead of silently reusing the old, non-comparable workload.

throw new Error(
    'scenarios/suite.js is retired - it exercised a workload with no bank-adapter parity ' +
    '(see docs/benchmark-remediation-guideline.md, findings F1/F5/F7). Use scenarios/suite-bsi.js.'
);
