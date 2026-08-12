/*
 * SM8 Core — CoreClasspathSpec.
 *
 * The ZERO-SPARK invariant, verified at test time.
 *
 * The Core's compile-time and test-time classpath must NOT contain
 * any org.apache.spark class. This spec walks the runtime classpath
 * looking for any class whose package starts with `org.apache.spark`
 * and fails the build if any is found.
 *
 * Pairs with the Maven Enforcer `bannedDependencies=org.apache.spark:*`
 * rule in pom.xml (compile-time guarantee). This spec is the test-time
 * guarantee: even if a transitive dependency slips through Maven
 * resolution, this test catches it.
 *
 * Per multi-engine-design.md §5.1 (semanticdf): the proven pattern for
 * Spark-free core verification. SM8 inherits it.
 *
 * Per karpathy-guidelines-mindset: define verifiable success criteria
 * before starting. The criterion is "sm8-core compiles and resolves
 * without org.apache.spark on the classpath, verified by Enforcer at
 * compile time AND CoreClasspathSpec at test time."
 */
package io.sm8.core

import java.io.File
import java.util.jar.JarFile
import java.util.stream.Collectors

import scala.jdk.CollectionConverters._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoreClasspathSpec extends AnyFlatSpec with Matchers {

  "sm8-core" should "have zero org.apache.spark classes on the runtime classpath" in {
    val sparkClasses = enumerateSparkClasses()
    sparkClasses shouldBe empty
  }

  /**
   * Walks the runtime classpath via the system ClassLoader. For each
   * URL pointing to a directory (IDE-style classpath), lists its
   * contents. For each JAR, lists its entries via java.util.jar.
   *
   * Returns any class whose FQN starts with `org.apache.spark`.
   *
   * This is intentionally conservative: if even one Spark class leaks
   * into the Core, the test fails. False-positives (e.g. a Spark class
   * in an unrelated test fixture) would also fail — that's a signal to
   * move the fixture out of sm8-core.
   */
  private def enumerateSparkClasses(): Seq[String] = {
    val cl = getClass.getClassLoader
    val urls = cl match {
      case u: java.net.URLClassLoader => u.getURLs.toSeq
      case _                          => Seq.empty
    }

    urls.flatMap { url =>
      val protocol = url.getProtocol
      val path = url.getPath
      if (protocol == "file" && new File(path).isDirectory) {
        // IDE classpath (target/classes, etc.)
        val root = new File(path)
        if (!root.exists) Seq.empty
        else walkDirectory(root, root)
          .map(relativeToClassName)   // Seq[File] -> Seq[String]
          .filter(isSparkClass)        // Seq[String] -> Seq[String]
      } else if (protocol == "file" && path.endsWith(".jar")) {
        // Maven shaded-jar-less install: each dep is its own jar
        try {
          val jar = new JarFile(new File(path))
          jar.stream()
            .filter(e => !e.isDirectory && e.getName.endsWith(".class"))
            .map(_.getName)
            .filter(isSparkClass)
            .collect(Collectors.toList[String])  // -> java.util.List<String>
            .asScala                              // -> scala.mutable.Buffer[String]
            .toSeq                                // -> Seq[String]
            .map(classNameFromJarEntry)
        } catch {
          case _: Exception => Seq.empty
        }
      } else Seq.empty
    }
  }

  private def walkDirectory(root: File, current: File): Seq[File] = {
    if (current.isFile) Seq(current)
    else current.listFiles().toSeq.flatMap(walkDirectory(root, _))
  }

  private def relativeToClassName(f: File): String = {
    val rootPath = f.getAbsolutePath
    val sep = java.io.File.separatorChar
    rootPath
      .replace(sep, '/')
      .replaceAll("""\.class$""", "")
      .dropWhile(_ != 'o') // trim path prefix; keep from "org/..." onward
  }

  private def classNameFromJarEntry(jarEntry: String): String =
    jarEntry.replaceAll("""\.class$""", "").replace('/', '.')

  private def isSparkClass(name: String): Boolean =
    name.startsWith("org.apache.spark") ||
      name.startsWith("org/apache/spark")
}