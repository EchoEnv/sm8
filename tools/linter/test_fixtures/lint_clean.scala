# Linter fixtures — synthetic inputs that the linter SHOULD flag

This file is checked into `tools/linter/test_fixtures/` to provide
stable negative inputs for the linter extension (PR-246). Run
`python3 tools/linter/check_scaladoc_noise.py tools/linter/test_fixtures/lint_clean.scala`
to confirm the linter does NOT flag these (they're either legitimate
or already-clean forms). For the new patterns added in PR-246, see
`should_be_flagged.scala`.
