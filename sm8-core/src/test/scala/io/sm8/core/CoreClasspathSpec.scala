/*
 * SM8 Core — CoreClasspathSpec.
 *
 * The ZERO-SPARK invariant, verified at test time.
 *
 * Audit fix (Step 3 audit): the previous implementation pattern-matched
 * on `URLClassLoader`, which silently returns empty on JDK 9+
 * (modern classloaders extend `BuiltinClassLoader`, not `URLClassLoader`).
 * That meant the ZERO-SPARK invariant was unverified on every modern
 * JVM — the test would pass even if Spark was on the classpath.
 *
 * New implementation walks `System.getProperty("java.class.path")`
 * which works on every JDK from 8 through 21+, regardless of
 * classloader hierarchy. Per [[debug-mantra-mindset]]: reproduce →
 * trace fail path → falsify hypothesis → cross-reference → verify.
 *
 * Audit fix: `case _: Exception` narrowed to `NonFatal(_)` so JVM
 * `Error`s (OutOfMemoryError etc.) propagate, and `listFiles()` is
 * NPE-safe (`Option(...).toSeq`).
 */
package io.sm8.core

import java.io.File
import java.io.IOException
import java.util.jar.JarFile
import java.util.zip.ZipException

import scala.util.control.NonFatal

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoreClasspathSpec extends AnyFlatSpec with Matchers {

  "sm8-core" should "have zero org.apache.spark classes on the runtime classpath" in {
    val sparkClasses = enumerateSparkClasses()
    sparkClasses shouldBe empty
  }

  /**
   * Walks the JVM classpath via `System.getProperty("java.class.path")`.
   * Works on every JDK from 8 through 21+, regardless of classloader
   * hierarchy. (The previous `URLClassLoader` pattern match failed
   * silently on JDK 9+.)
   *
   * Each entry is either a directory (IDE / Maven `target/classes`)
   * or a JAR. Returns any class whose FQN starts with `org.apache.spark`.
   */
  private def enumerateSparkClasses(): Seq[String] = {
    val sep    = File.pathSeparatorChar
    val paths  = System.getProperty("java.class.path").split(sep).toSeq
    paths.flatMap(processClasspathEntry)
  }

  private def processClasspathEntry(path: String): Seq[String] = {
    val file = new File(path)
    if (file.isDirectory) collectClassesFromDir(file)
    else if (path.toLowerCase.endsWith(".jar")) {
      try enumerateJar(file)
      catch {
        case NonFatal(_) => Seq.empty  // corrupt / unreadable JAR — skip
      }
    } else Seq.empty
  }

  /**
   * Recursively walk a directory tree, returning the names of all
   * Spark classes found. `Option(listFiles)` makes the walk NPE-safe
   * (a directory with restricted perms returns null).
   */
  private def collectClassesFromDir(dir: File): Seq[String] = {
    Option(dir.listFiles).toSeq.flatMap { children =>
      children.toSeq.flatMap { f =>
        if (f.isFile) {
          val name = relativeToClassName(f)
          if (isSparkClass(name)) Some(name) else None
        } else collectClassesFromDir(f)
      }
    }
  }

  /**
   * Enumerate class entries in a JAR. Returns names of any
   * `org.apache.spark.*` classes found.
   */
  private def enumerateJar(file: File): Seq[String] = {
    import scala.jdk.CollectionConverters._
    val jar = new JarFile(file)
    try {
      jar.stream()
        .filter(e => !e.isDirectory && e.getName.toLowerCase.endsWith(".class"))
        .map(_.getName)
        .filter(isSparkClass)
        .collect(java.util.stream.Collectors.toList[String])
        .asScala
        .toSeq
        .map(classNameFromJarEntry)
    } finally jar.close()
  }

  private def relativeToClassName(f: File): String = {
    val rootPath = f.getAbsolutePath.replace(File.separatorChar, '/')
    val idx = rootPath.indexOf("/org/")
    if (idx < 0) ""
    else rootPath.substring(idx + 1).replaceAll("""\.class$""", "")
  }

  private def classNameFromJarEntry(jarEntry: String): String =
    jarEntry.replaceAll("""\.class$""", "").replace('/', '.')

  private def isSparkClass(name: String): Boolean =
    name.startsWith("org.apache.spark") ||
      name.startsWith("org/apache/spark")
}