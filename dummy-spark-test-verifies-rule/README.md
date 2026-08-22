# dummy-spark-test-verifies-rule

Per **ADR-008-AD v1.1**, this is the executable verification harness for the Zero-Spark invariant enforced by `maven-enforcer-plugin` `bannedDependencies`.

## What this is

This directory contains:
- `pom.xml` — a Maven module with `<dependency>org.apache.spark:spark-core_2.13</dependency>` (the intended violation)
- `verify-rule-fires.sh` — an executable shell script that runs `mvn validate` on the dummy module and asserts the enforcer rule fires

## Important caveat (per the senior dual review of Wave 2)

**The parent's `<build><plugins>` enforcer config does NOT auto-apply executions to child modules.** Per the maven.apache.org FAQ, children must explicitly re-declare the plugin to inherit executions.

This means:
- If the **per-module `<plugin><maven-enforcer-plugin>...</maven-enforcer-plugin></plugin>` block is intact** in `sm8-core/pom.xml` (etc.), the rule fires on the dummy module's `mvn validate`.
- If a future contributor **deletes the per-module block** (e.g. when copying the parent for a new module), the rule stops firing — and the dummy module's `verify-rule-fires.sh` will report FAIL.

The dummy module is intentionally NOT registered in the parent reactor (`<modules>` block in `pom.xml`). This keeps the default reactor build clean (doesn't trigger the violation) while still allowing the verification script to run manually.

## Usage

```bash
# Should FAIL with "Zero-Spark invariant" message
bash dummy-spark-test-verifies-rule/verify-rule-fires.sh
```

If `Maven exit code: 0` is reported, the Zero-Spark invariant is NOT being enforced — investigate immediately.

If the script reports `PASS: the enforcer rule fired as expected`, the invariant is intact.

## Per-module enforcer block

The per-module `<plugin><maven-enforcer-plugin>...` block in each non-spark-connector `pom.xml` is:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-enforcer-plugin</artifactId>
  <version>3.4.1</version>
  <executions>
    <execution>
      <id>enforce-no-spark</id>
      <goals><goal>enforce</goal></goals>
    </execution>
  </executions>
  <configuration>
    <rules>
      <bannedDependencies>
        <excludes>
          <exclude>org.apache.spark:*</exclude>
        </excludes>
        <message>
          The Zero-Spark invariant (ADR-008-AD v1.0): every reactor
          module must remain Spark-free EXCEPT the spark-connector
          (which opts out via skip=true in its own pom.xml). If
          you need a Spark class, add the dependency to
          connectors/spark-connector/pom.xml instead.
        </message>
      </bannedDependencies>
    </rules>
  </configuration>
</plugin>
```

## See also

- `docs/adr/0008-ad-parent-pom-banneddependencies-hoist.md` v1.1
