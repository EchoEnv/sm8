/*
 * SM8 Platform — QueryValidationService (v2 D1: request-dim checking).
 *
 * Execute-free query validation: answers "would this run cleanly
 * against this Model?" without invoking the engine, without firing
 * pipeline hooks, and without touching the result cache.
 *
 * == Why a standalone service (not a handler on QueryService) ==
 *
 * Per the codebase's existing precedent (one ServiceDefinition per
 * verb-class — QueryService, ModelService, MetricsService,
 * EngineService, MetaInspectorService): `QueryService` is a stateful
 * executor (cache + rollup rewrite + engine dispatch); validate is
 * an idempotent read-only projection. Mixing them in one service
 * conflates two contracts. The service is bound additively in
 * `HttpTransport` using the same pattern.
 *
 * == Write safety: by construction, not by gating ==
 *
 * The handler calls three pure core functions directly —
 * `ModelValidator.validate`, `QueryBuilder.build`, and
 * `RollupRewriter.rewrite` — and nothing else. No Engine, no
 * `EngineHookDispatcher`, no pipeline stages, no cache plugin, no
 * DataFrame. There is no hook wiring to mis-configure: the class of
 * "validate accidentally triggered a write" bugs is structurally
 * absent. (Grilling rounds 1–3 on decision ticket #407 rejected
 * reusing `runQueryWithHooks` with a `Context.stop` marker because
 * the dispatcher's PreExecute cache read fires unconditionally and
 * post-hooks with `runsOnStop = true` fire even on stop.)
 *
 * == DeclaredSchemaResolver ==
 *
 * `QueryBuilder.build` requires a `SourceResolver`; the only shipped
 * implementation is `SparkSourceResolver` (Spark IO). Validate must
 * not touch Spark. The nested `DeclaredSchemaResolver` implements the
 * core trait by returning `ResolvedSource.Scan` carrying the field
 * set the MODEL itself declares — dimensions (with their
 * `SealedDataType`, defaulting to `Varchar` when absent) plus measures
 * (untyped by contract; `RollupRewriter` falls back to `Varchar` for
 * untyped measure refs, verified RollupRewriter.scala:1095). This is
 * "trust the manifest's declaration" semantics: validate checks the
 * query against the model's CONTRACT, not against the warehouse's
 * current state.
 *
 * == D1: request-dim / request-measure cross-check ==
 *
 * v1 (merged #410) passed unknown request dimensions silently —
 * `QueryBuilder.build` doesn't consume `request.dimensions` and
 * there's no core check. This v2 addition catches operator typos
 * (e.g. "reigon") and stale refs BEFORE the build. Pure data
 * diff against the Model's declared names — no IO. Unknown
 * dims/measures surface as `ValidationFailure(stage = "request")`
 * with one `EngineError.UnsupportedCapability` per unknown name.
 *
 * == Drift detection is OUT of scope ==
 *
 * Warehouse-side schema drift (dropped column, widened type) is NOT
 * surfaced here. The typed backstop `ModelValidator.validateAgainstSchema`
 * exists in core but has zero production callers (verified 2026-09-12,
 * seedling r5 #2) — wiring it into the connector execute path is a
 * named follow-up (tracked as Q8 on #407).
 *
 * == Spark concerns ==
 *
 * None: no Spark types, no DataFrame, no session. DeclaredSchemaResolver
 * is pure data synthesis from the captured `Model`. Safe under
 * concurrent invocation (all state is the immutable captured `Model`).
 */
package io.sm8.platform.query

import io.sm8.core.engine.{EngineError, EngineIdentity, QueryRequest, ResolvedSource, SourceResolver}
import io.sm8.core.model.{Model, ModelValidationError, ModelValidator, SourceRef}
import io.sm8.core.query.QueryBuilder
import io.sm8.core.rel.RollupRewriter
import io.sm8.core.schema.Field

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import dev.restate.sdk.HandlerRunner
import dev.restate.sdk.common.TerminalException
import dev.restate.sdk.endpoint.definition.{
  HandlerDefinition,
  HandlerType,
  ServiceDefinition,
  ServiceType
}
import dev.restate.serde.jackson.JacksonSerdeFactory

/** Successful validation result: what the execute WOULD have done.
  *
  * @param modelVersion   the `Model.version` that was validated
  * @param rollupDecision the `RollupRewriter` outcome — `Rewritten`
  *                       if the query routes to a rollup, `Unchanged`
  *                       otherwise (Unchanged is a valid outcome, not
  *                       a failure; the `reason` carries the typed
  *                       refusal)
  * @param decisionHints  broadcast/skew hints; always `None` under
  *                       validate (hints are populated from
  *                       `context.meta` by PreExecute hooks, which
  *                       validate does not run). Kept as `Option` for
  *                       v2 shape-stability.
  * @param engineSelection the engine that WOULD serve the query
  * @param tablesTouched  physical tables the execute WOULD read (model
  *                       source + join right-sides)
  */
final case class ValidationOutcome(
  modelVersion: Int,
  rollupDecision: RollupRewriter.RollupRewriteResult,
  decisionHints: Option[io.sm8.core.engine.DecisionHints],
  engineSelection: String,
  tablesTouched: List[String],
  /** D3: the SQL the execute WOULD issue. Populated when the
    * deployment supplies a compiledSqlFn; `None` when absent
    * (v1 semantics preserved). */
  compiledSql: Option[String] = None
)

/** Typed validation failure: which stage failed and why.
  *
  * @param stage  "request" (D1: unknown request dims/measures),
  *               "model" (model-level integrity), "build" (query
  *               well-formedness / resolution), or "drift" (D2:
  *               warehouse schema drift vs declared schema)
  * @param errors all collected errors for that stage (aggregated,
  *               not first-only)
  */
final case class ValidationFailure(
  stage: String,
  errors: List[EngineError]
) {
  /** One-line human-readable summary of the failure for the wire
    * error path (used in `TerminalException(400, message)`).
    *
    * @return the composed summary string (never null)
    */
  def message: String = s"[$stage] " + errors.map(_.toString).mkString("; ")
}

/** The service. Construct with the deployment's captured `Model`.
  * Stateless: all state is the immutable captured `Model`.
  */
object QueryValidationService {

  /** Synthetic engine identity for validate invocations — pinned so
    * validate shows up distinctly in logs and audit events (per
    * decision ticket #407 Q6).
    */
  val ValidateEngineIdentity: EngineIdentity =
    EngineIdentity(
      name = "validate",
      nativeVersion = "0.0",
      engineAdapterVersion = "sm8-validate/0.1.0"
    )

  /** The core pipeline prefix validate runs (documented contract;
    * see class Scaladoc for what it deliberately excludes).
    *
    * @param model   the deployment's captured Model
    * @param request the incoming query request
    */
  private[query] def runValidation(
      model: Model,
      request: QueryRequest,
      declaredFields: List[Field] = Nil,
      compiledSqlFn: Option[Model => Either[EngineError, String]] = None
  ): Either[ValidationFailure, ValidationOutcome] = {
    // Stage 0 (D1): request-vs-model cross-check. Catches operator
    // typos and stale refs BEFORE the relop build. Pure data diff
    // against the Model's declared names; no engine, no hooks, no IO.
    // Aggregates ALL unknown refs into one typed ValidationFailure.
    val unknown = unknownRefs(request, model)
    if (unknown.nonEmpty) {
      return Left(ValidationFailure(
        stage = "request",
        errors = unknown.map { case (kind, name) =>
          EngineError.UnsupportedCapability(
            engine = "validate",
            capability = s"unknown-$kind",
            message = s"unknown $kind in request: '$name' (not declared on model '${model.name}' v${model.version})"
          )
        }
      ))
    }
    // Stage 1: model-level integrity (cross-refs, duplicate names,
    // calc-measure DAG, rollup-ref existence). Aggregates ALL errors.
    ModelValidator.validate(model) match {
      case Left(err: ModelValidationError) =>
        Left(ValidationFailure(
          stage = "model",
          errors = List(EngineError.UnsupportedCapability(
            engine = "validate",
            capability = "model-integrity",
            message = err.toString
          ))
        ))
      case Right(_) =>
        // Stage 2 + 3: build the relop against the DECLARED schema,
        // then preview the rollup routing. build consumes the
        // DeclaredSchemaResolver (pure synthesis — no Spark IO);
        // rewrite is pure plan → plan.
        val resolver = new DeclaredSchemaResolver(model)
        QueryBuilder.build(model, resolver, ValidateEngineIdentity) match {
          case Left(buildErr) =>
            Left(ValidationFailure(stage = "build", errors = List(buildErr)))
          case Right(plan) =>
            val decision = RollupRewriter.rewrite(plan, model, request.timeGrain)

            // Stage 3b (D2): warehouse schema-drift detection. When the
            // deployment supplies a live schema (captured at definition
            // time from the connector's actual warehouse state), run
            // `validateAgainstSchema` to surface dropped/widened columns
            // as typed ValidationFailure(stage = "drift") errors. When
            // no live schema is provided, drift detection is skipped
            // (v1 semantics preserved — validate still reports the
            // DeclaredSchemaResolver's synthesized fields).
            // D2: schema-drift detection (when a live schema is provided).
            val driftCheck: Either[ValidationFailure, Unit] =
              if (declaredFields.isEmpty) Right(())
              else {
                val liveSchema = ResolvedSource.Scan(
                  source = model.source,
                  schema = declaredFields
                )
                ModelValidator.validateAgainstSchema(model, liveSchema).left.map { err =>
                  ValidationFailure(
                    stage = "drift",
                    errors = List(EngineError.UnsupportedCapability(
                      engine = "validate",
                      capability = "schema-drift",
                      message = err.toString
                    ))
                  )
                }
              }

            // D3: compile SQL INSIDE the drift-pass branch. When drift
            // fails, the callback must NEVER be invoked — a SQL
            // preview generated for a drifted schema would mislead
            // an operator into thinking the drifted query is OK.
            // Drift short-circuits the Either chain so the callback
            // is unreachable in the Left(drift) case.
            driftCheck.flatMap { _ =>
              val compiledSql: Option[String] = compiledSqlFn match {
                case Some(fn) =>
                  fn(model) match {
                    case Right(sql) => Some(sql)
                    case Left(_)    => None // silent (compile failure is not validation failure)
                  }
                case None => None
              }
              Right(ValidationOutcome(
                modelVersion = model.version,
                rollupDecision = decision,
                decisionHints = None, // hints come from PreExecute hooks; validate runs none
                engineSelection = "default",
                tablesTouched = tablesTouched(model),
                compiledSql = compiledSql
              ))
            }
        }
    }
  }

  /** Physical tables the execute WOULD read: the model's primary
    * source plus every join's right side. Derived from the Model's
    * own declarations. */
  private[query] def tablesTouched(model: Model): List[String] = {
    val primary = model.source match {
      case io.sm8.core.model.SourceRef.ByName(_, _, table) => List(table)
      case io.sm8.core.model.SourceRef.ByPath(format, path, _) =>
        List(s"$format:$path")
      case io.sm8.core.model.SourceRef.ByProvider(providerRefName) =>
        List(s"provider:$providerRefName")
    }
    primary ++ model.joins.map(_.rightModel)
  }

  /** Stage-0 request-vs-model cross-check: returns every (kind, name)
    * pair in the request that the Model does not declare. Pure data
    * diff; no IO. Kinds: "dimension", "measure".
    *
    * @param request the incoming query request
    * @param model   the deployment's captured Model
    * @return        the unknown (kind, name) pairs, in request order
    */
  private[query] def unknownRefs(
      request: QueryRequest,
      model: Model
  ): List[(String, String)] = {
    val declaredDims = model.dimensions.map(_.name).toSet
    // CalculatedMeasures are also resolvable request-level measures
    // (mirrors DeclaredSchemaResolver's field synthesis, which includes
    // calc-measures to prevent false-positive "unknown measure" errors
    // for names the model legitimately serves).
    val declaredMeas =
      (model.measures.map(_.name) ++ model.calculatedMeasures.map(_.name)).toSet
    request.dimensions.filterNot(declaredDims).map("dimension" -> _).toList ++
    request.measures.filterNot(declaredMeas).map("measure" -> _).toList
  }

  /** Synthesize the declared field set from the Model itself.
    * Dimensions carry `SealedDataType` (default `Varchar` when the
    * manifest omitted it); measures are untyped by contract and
    * default to `Varchar` (matches `RollupRewriter`'s own fallback,
    * RollupRewriter.scala:1095). */
  private[query] final class DeclaredSchemaResolver(model: Model) extends SourceResolver {
    private val declaredFields: List[Field] =
      model.dimensions.map { d =>
        Field(
          name = d.name,
          dataType = d.dataType.getOrElse(io.sm8.core.schema.SealedDataType.Varchar),
          nullable = true
        )
      } ++
      model.measures.map { m =>
        Field(
          name = m.name,
          dataType = io.sm8.core.schema.SealedDataType.Varchar,
          nullable = true
        )
      } ++
      model.calculatedMeasures.map { cm =>
        Field(
          name = cm.name,
          dataType = io.sm8.core.schema.SealedDataType.Varchar,
          nullable = true
        )
      }

    /** Trust the manifest's declaration: return the model's own
      * field set regardless of what the warehouse currently has.
      * Drift is an execute-time concern (see class Scaladoc).
      *
      * @param source   the model's primary source ref (consumed into
      *                 the `Scan` for provenance)
      * @param identity the engine identity (validate passes the
      *                 pinned `ValidateEngineIdentity`)
      * @return         `Right(ResolvedSource.Scan(source,
      *                 declaredFields))` always
      */
    override def resolve(
        source: SourceRef,
        identity: EngineIdentity
    ): Either[EngineError, ResolvedSource] =
      Right(ResolvedSource.Scan(source = source, schema = declaredFields))
  }

  /** Build the Restate `ServiceDefinition` for the `validate`
    * handler.
    *
    * @param model           the deployment's captured Model
    * @param declaredFields  the live warehouse schema (D2 drift
    *                        detection); `Nil` = skip the drift check
    *                        (v1 semantics preserved)
    * @param compiledSqlFn   deployment-supplied SQL compiler (D3
    *                        preview); `None` = `compiledSql` stays
    *                        `None` in the outcome
    * @return                the `ServiceDefinition` exposing `validate`
    */
  def definition(
      model: Model,
      declaredFields: List[Field] = Nil,
      compiledSqlFn: Option[Model => Either[EngineError, String]] = None
  ): ServiceDefinition = {
    val scalaMapper: ObjectMapper =
      new ObjectMapper().registerModule(DefaultScalaModule)
    val jacksonSerdeFactory = new JacksonSerdeFactory(scalaMapper)

    val validateRequestSerde = jacksonSerdeFactory.create(classOf[QueryRequest])
    val validateResponseSerde = jacksonSerdeFactory.create(classOf[ValidationOutcome])

    val validateRunner: HandlerRunner[QueryRequest, ValidationOutcome] =
      HandlerRunner.of(
        (_: dev.restate.sdk.Context, req: QueryRequest) =>
          runValidation(model, req, declaredFields, compiledSqlFn) match {
            case Right(outcome) => outcome
            case Left(failure) =>
              // Typed validation failure → the wire error. Restate
              // surfaces non-TerminalException throws as retriable
              // 500s; a validation refusal is NOT retriable (the
              // answer is deterministic), so wrap in a 400-class
              // TerminalException.
              throw new TerminalException(400, failure.message)
          },
        jacksonSerdeFactory,
        HandlerRunner.Options.DEFAULT
      )

    ServiceDefinition.of(
      "QueryValidationService",
      ServiceType.SERVICE,
      java.util.List.of(
        HandlerDefinition.of(
          "validate",
          HandlerType.SHARED,
          validateRequestSerde,
          validateResponseSerde,
          validateRunner
        )
      )
    )
  }
}
