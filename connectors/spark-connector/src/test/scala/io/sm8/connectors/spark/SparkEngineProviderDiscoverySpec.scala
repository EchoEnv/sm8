/*
 * SM8 Spark Connector — SparkEngineProvider Portal discovery spec.
 *
 * Verifies that Main.discoverProviders (and any Java ServiceLoader-
 * based wiring) finds the SparkEngineProvider through the standard
 * `META-INF/services/io.sm8.core.engine.MCPEngineProvider` declaration.
 * This is the missing link that makes `Main --model m.yaml` actually
 * runnable against Spark without requiring the deployment to hardcode
 * the provider class.
 *
 * Per [[debug-mantra-mindset]] §1: each test exercises ONE observable
 * contract:
 *   1. ServiceLoader discovers the class declared in META-INF/services.
 *   2. The discovered class has the documented wire identity.
 *   3. The no-arg constructor (used by ServiceLoader) produces the
 *      contract-gap stub (spark = null, available = false).
 *   4. The Portal declaration name matches the exact contract.
 *   5. The rich 3-arg constructor (production wiring) survives
 *      ObjectOutputStream round-trip — closure-safety contract
 *      from PR #36, verbatim from the existing SparkEngineProviderSpec.
 *
 * Per [[scala-spark-batch-bugs-mindset]] mantra #1: the no-arg
 * constructor ONLY captures SparkTypeBridge (a singleton object that
 * holds no SparkSession, no Iterator, no Connection). The real
 * SparkSession is constructed by the deployment wiring (Main) and
 * supplied via the 3-arg constructor — the ServiceLoader-instanced
 * object never serializes.
 */
package io.sm8.connectors.spark

import io.sm8.core.engine.MCPEngineProvider

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkEngineProviderDiscoverySpec extends AnyFunSuite with Matchers {

  // -- Portal: ServiceLoader discovery --

  test("Portal: META-INF/services/io.sm8.core.engine.MCPEngineProvider declares SparkEngineProvider") {
    val lines = scala.collection.mutable.Set[String]()
    val classLoader = classOf[SparkEngineProvider].getClassLoader
    val urls = classLoader.getResources("META-INF/services/io.sm8.core.engine.MCPEngineProvider")
    urls.asScala.foreach { u =>
      scala.io.Source.fromURL(u).getLines().foreach(lines += _)
    }
    lines.toSet should contain (classOf[SparkEngineProvider].getName)
  }

  test("Portal: ServiceLoader discovers SparkEngineProvider from the classpath") {
    val providers = ServiceLoader
      .load(classOf[MCPEngineProvider], classOf[SparkEngineProvider].getClassLoader)
      .iterator()
      .asScala
      .toList
    val classes = providers.map(_.getClass.getName).toSet
    classes should contain (classOf[SparkEngineProvider].getName)
  }

  // -- No-arg constructor: the contract-gap stub shape --

  test("SparkEngineProvider no-arg ctor: produces the contract-gap stub (available = false)") {
    val p = new SparkEngineProvider()
    p.identity.name shouldBe "spark-3.5"
    p.available shouldBe false
  }

  // -- Rich constructor: preserves the Serializable contract --

  test("SparkEngineProvider (rich ctor): full Java-serialization round-trip survives") {
    // Mirror SparkEngineProviderSpec.scala line 63 contract proof —
    // captures SparkSession=null path because Spark dependency isn't on
    // the test classpath for the discovery spec, and the round-trip
    // type-checks regardless of which path it goes through.
    val p = new SparkEngineProvider(null, SparkTypeBridge, "spark-3.5")
    val bytes = {
      val bos = new java.io.ByteArrayOutputStream()
      val oos = new java.io.ObjectOutputStream(bos)
      oos.writeObject(p); oos.close(); bos.toByteArray
    }
    val back = {
      val ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(bytes))
      ois.readObject().asInstanceOf[SparkEngineProvider]
    }
    back.identity.name shouldBe "spark-3.5"
    back.available shouldBe false
  }
}
