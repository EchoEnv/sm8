package io.sm8.core.engine

import io.sm8.core.model.Model
import io.sm8.sdk.{ Request, Result }

final case class EngineHookRequest(
    model:      Model,
    mcpRequest: MCPQueryRequest,
    cacheKey:   String
) extends Request

final case class EngineHookResult(
    pqr: PortableQueryResult
) extends Result
