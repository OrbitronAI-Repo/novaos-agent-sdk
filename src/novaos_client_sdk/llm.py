"""LLM Gateway client for NovaOS agents.

Wraps the standard ``openai.AsyncOpenAI`` client, configured to route
through the NovaOS LLM Gateway using execution tokens for authentication.
Same pattern as ``MCPClient`` for MCP Gateway access.

The LLM Gateway speaks the OpenAI API protocol — agents use the standard
OpenAI SDK interface (chat.completions, embeddings, models) without
knowing about provider routing, fallbacks, or budget enforcement happening
behind the gateway.

Example:
    ```python
    from novaos_client_sdk.context import ExecutionContext
    from novaos_client_sdk.llm import LLMClient

    ctx = ExecutionContext.from_headers(request.headers)

    async with LLMClient(execution_context=ctx) as llm:
        # Standard OpenAI SDK — streaming chat completion
        stream = await llm.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "Hello"}],
            stream=True,
        )
        async for chunk in stream:
            print(chunk.choices[0].delta.content, end="")

        # Embeddings
        result = await llm.embeddings.create(
            model="text-embedding-3-small",
            input="Some text to embed",
        )

        # List available models (scope-filtered by gateway)
        models = await llm.available_models()
    ```
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

from novaos_client_sdk.context import ExecutionContext
from novaos_client_sdk.exceptions import NovaOSError

if TYPE_CHECKING:
    from openai import AsyncOpenAI
    from openai.resources.chat import AsyncChat
    from openai.resources.embeddings import AsyncEmbeddings
    from openai.resources.models import AsyncModels

logger = logging.getLogger(__name__)

LLM_GATEWAY_URL_ENV = "LLM_GATEWAY_URL"


class LLMClient:
    """Async client for the NovaOS LLM Gateway.

    Wraps ``openai.AsyncOpenAI`` with:
    - ``base_url`` pointing to the LLM Gateway (not OpenAI directly)
    - ``api_key`` set to the execution token (validated by gateway's custom_auth)
    - ``X-NovaOS-*`` default headers for observability correlation

    The returned object exposes the full ``openai.AsyncOpenAI`` interface:
    ``chat.completions``, ``embeddings``, ``models``, etc.

    Args:
        base_url: LLM Gateway URL. If None, reads from ``LLM_GATEWAY_URL`` env var.
        execution_context: ExecutionContext with the execution token for auth.
            If None, the client will not send auth headers (useful for local dev
            with auth disabled on the gateway).
        timeout: Request timeout in seconds (default: 600, matches OpenAI SDK default).
    """

    def __init__(
        self,
        base_url: str | None = None,
        execution_context: ExecutionContext | None = None,
        timeout: float = 600.0,
    ) -> None:
        try:
            from openai import AsyncOpenAI
        except ImportError as err:
            raise NovaOSError(
                "The 'openai' package is required for LLMClient. "
                "Install it with: pip install novaos-agent-sdk[llm]"
            ) from err

        if base_url is None:
            base_url = os.getenv(LLM_GATEWAY_URL_ENV)
            if not base_url:
                raise NovaOSError(
                    f"LLM Gateway URL not provided and '{LLM_GATEWAY_URL_ENV}' "
                    "environment variable is not set."
                )

        # Ensure base_url ends with /v1 for OpenAI SDK compatibility
        if not base_url.endswith("/v1"):
            base_url = base_url.rstrip("/") + "/v1"

        # The execution token is used as the "api_key" — the gateway's
        # custom_auth validates it as an RS256 JWT, not an OpenAI key.
        # When execution_context is None the caller explicitly opted out of auth
        # (local dev with auth disabled on the gateway).  When a context IS
        # provided it must carry a token — a context without a token means the
        # auth-proxy header was missing and the call would be rejected anyway.
        api_key = "novaos-no-auth"  # Placeholder for no-auth local dev only
        default_headers: dict[str, str] = {}

        if execution_context is not None:
            api_key = execution_context.require_token()
            default_headers = execution_context.to_context_headers()

        self._client: AsyncOpenAI = AsyncOpenAI(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            max_retries=0,
            default_headers=default_headers,
        )

    # Expose openai.AsyncOpenAI sub-clients directly

    @property
    def chat(self) -> AsyncChat:
        """Access chat completions (``client.chat.completions.create(...)``)."""
        return self._client.chat

    @property
    def embeddings(self) -> AsyncEmbeddings:
        """Access embeddings (``client.embeddings.create(...)``)."""
        return self._client.embeddings

    @property
    def models(self) -> AsyncModels:
        """Access model listing (``client.models.list()``)."""
        return self._client.models

    # --- Model discovery (scope-filtered by LLM Gateway) ---

    async def available_models(self) -> list[str]:
        """Returns model names the caller has access to.

        Calls /v1/models on the LLM Gateway, which filters the response
        based on the caller's ``llm:model:*`` scopes in the execution token.
        Only models the caller is authorized to use are returned.

        Returns:
            List of model name strings (e.g., ["gpt-4o", "gpt-4o-mini"]).
        """
        response = await self._client.models.list()
        return sorted(m.id for m in response.data)

    async def has_model(self, model: str) -> bool:
        """Check if a specific model is available to the caller.

        Args:
            model: Model name to check (e.g., "gpt-4o").

        Returns:
            True if the caller has scope-based access to the model.
        """
        available = await self.available_models()
        return model in available

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.close()

    async def __aenter__(self) -> LLMClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
