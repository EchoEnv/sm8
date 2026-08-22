# dummy-spark-test-verifies-rule

Per **ADR-008-AD v1.1**, this is the negative verification test for the Zero-Spark invariant enforced by `maven-enforcer-plugin` `bannedDependencies`.

**This module is intentionally kept in the reactor with a Spark dependency.** Running `mvn validate` on this module MUST FAIL with the Zero-Spark invariant error message.

## Why this exists

A future contributor who:
1. Removes the per-module `enforce-no-spark` block from `sm8-core/pom.xml` (or any other non-spark-connector module)
2. Removes the parent's `<pluginManagement>` template

...would silently allow Spark to leak into non-spark modules. This dummy module ensures that any future violation is caught at build time.

## Usage

```bash
# Should FAIL with "Zero-Spark invariant" error message
mvn -B -ntp -pl dummy-spark-test-verifies-rule validate
```

If `validate` succeeds, the Zero-Spark invariant is NOT being enforced — investigate immediately.

## Why we keep this module OUT of the reactor

This module contains an `org.apache.spark:spark-core_2.13` dependency that VIOLATES the Zero-Spark invariant. It is intentionally NOT registered in the parent pom.xml's `<modules>` block (and therefore not in the default reactor build) so that:
- The default reactor build (`mvn test`) is not blocked by this violation
- The negative test is invoked manually with `mvn -pl dummy-spark-test-verifies-rule validate`

To make this module part of the regular build verification, add it to the parent `<modules>` block temporarily — but that would block the regular reactor build.

## See also

- `docs/adr/0008-ad-parent-pom-banneddependencies-hoist.md` v1.1
