# Authentication

The SDK never creates, stores, or refreshes credentials itself — it only formats whatever token or key your application already has into the right header. Which pattern you need depends on which side you're on, and whether your credential is static for the life of a `Client` or needs to change mid-session.

## Calling an agent or the gateway: `AuthProvider`

Every client (`Client`, `GatewayClient`, `AgentDNSClient`) accepts an `auth` argument implementing [`AuthProvider`](../reference/api.md#novaos_client_sdk.auth). It's read once, at construction, and formatted into every request's headers from then on. Two implementations are built in.

**Bearer tokens** — for dynamic, short-lived tokens (OAuth/OIDC access tokens, NovaOS IAM tokens for the Execution Gateway). Never store these in environment variables; your application supplies them at call time.

```python
from novaos_client_sdk import Client, BearerTokenAuth

token = my_app.get_access_token()  # however your app obtains one
auth = BearerTokenAuth(token=token)

async with Client(base_url="http://my-agent.example.internal:8000", auth=auth) as client:
    ...
```

**API keys** — for static, service-to-service credentials. These are safe to source from the environment:

```python
from novaos_client_sdk import APIKeyAuth, get_api_key_from_env

# Explicit
auth = APIKeyAuth(api_key="my-key", header_name="X-API-Key")

# From the ACP_API_KEY environment variable
auth = get_api_key_from_env()
```

**A custom provider** — implement the one-method interface for anything else (mTLS-derived headers, a signed-request scheme, ...):

```python
from novaos_client_sdk import AuthProvider

class CustomAuth(AuthProvider):
    def get_headers(self) -> dict[str, str]:
        return {"X-Custom-Auth": my_signing_function()}
```

## A credential that changes mid-session: `AuthInterceptor`

`auth=` above is read once and can't hand back a different credential later — fine for a static API key, not for a client-credentials OAuth2 token that's about to expire. `Client` also accepts `interceptors=`, a list of [`ClientCallInterceptor`](../reference/api.md#novaos_client_sdk.interceptors) instances that run around every call. [`AuthInterceptor`](../reference/api.md#novaos_client_sdk.auth_interceptor) is the built-in one for credentials, and it resolves a header **per call**, in this order:

1. If a `CredentialService` was configured, `AuthInterceptor` picks which of the agent's Agent Card `securitySchemes` this call should use (matching the card's declared `securityRequirements`, or the first declared scheme if there's nothing to match against) and calls `await credential_service.get_credentials(scheme_name, context)`.
2. If that returns a credential, it's formatted into a header using the matched scheme's shape — an `apiKeySecurityScheme` becomes its declared header name, everything else (including NovaOS's own execution-token scheme and OAuth2) becomes `Authorization: Bearer <credential>`. A service that already knows its own header shape can return a ready-made `{header: value}` mapping instead, used unchanged.
3. Otherwise — no `CredentialService` configured, or one configured that returned nothing — it falls back to the constructor's `AuthProvider`, so a caller who supplies only `auth=` sees no change at all.

```python
from novaos_client_sdk import Client, AuthInterceptor, CredentialService

class LogtoTokenService(CredentialService):
    def __init__(self, logto_client) -> None:
        self._logto_client = logto_client

    async def get_credentials(self, scheme_name, context):
        # Your own refresh logic decides whether the cached token is still
        # good; the SDK never sees that logic, only the result.
        return await self._logto_client.get_access_token()

interceptor = AuthInterceptor(credential_service=LogtoTokenService(logto_client))

async with Client(base_url="http://my-agent.example.internal:8000", interceptors=[interceptor]) as client:
    ...
```

The application still owns obtaining the credential — `CredentialService` is only the hook it refreshes through. For the common case of a credential your application already has per-session (its own OAuth2 flow) with nothing to refresh, `InMemoryContextCredentialStore` is a ready-made lookup table keyed by `context.state["session_id"]` and scheme name — populate it with `set_credentials(session_id, scheme_name, credential)` instead of writing a `CredentialService`.

## Inside an agent: the execution context

An agent running on the NovaOS platform sits behind an auth-proxy sidecar that injects the caller's identity and a short-lived execution token as headers on every request — you don't mint or manage these yourself. [`AgentExecutionContext`](../reference/api.md#novaos_client_sdk.agent_context) (the `ctx` your skill receives) parses them for you:

```python
from novaos_client_sdk import AgentExecutionContext

async def my_skill(ctx: AgentExecutionContext, input_text: str):
    print(f"user={ctx.user_id} tenant={ctx.tenant_id} run={ctx.run_id}")

    # Propagate the execution token to a downstream call your skill makes directly
    outbound_headers = ctx.to_execution_context().to_auth_headers()
    # {"Authorization": "Bearer <execution-token>"}
```

`ctx.tools.*` (see [Platform tools](platform-tools.md)) already attach this token automatically — you only need `to_auth_headers()` for calls the SDK doesn't make on your behalf.

Running the agent locally, outside the sidecar? There's no token to inject, so calls that require one raise `MissingExecutionTokenError`. Set the `NOVAOS_EXECUTION_TOKEN` environment variable to a test value, or pass headers explicitly, to unblock local development.

## Per-run secrets: sealed delivery

The platform can also hand your agent secrets scoped to one run — an SMTP password, a third-party API key — without ever putting the plaintext on the wire in the clear. Its Temporal worker seals each value to your pod's own key before the request leaves the control plane; the sealed payload rides inside the A2A `user-context/v1` extension and is opened only inside your process, past the auth-proxy sidecar. You don't need to know any of that to use it: call `ctx.get_secret("SECRET_NAME")` (optionally with a `default` for one that may not be delivered) from any skill, and a `SecretNotDeliveredError` tells you if nothing arrived under that name. This is entirely platform-driven — nothing in your agent code chooses what gets sealed or when.

## Verifying tokens: `JWKSTokenVerifier`

If your agent needs to verify an execution token itself (rather than trusting the sidecar), [`JWKSTokenVerifier`](../reference/api.md#novaos_client_sdk.token_verifier) fetches and caches signing keys from the gateway's JWKS endpoint. Requires the `crypto` extra:

```bash
pip install "novaos-agent-sdk[crypto]"
```

```python
import os
from novaos_client_sdk import JWKSTokenVerifier

verifier = JWKSTokenVerifier(
    gateway_url=os.environ["GATEWAY_URL"],
    issuer=os.environ["NOVAOS_TOKEN_ISSUER"],  # required — see below
)

token = request.headers["Authorization"].removeprefix("Bearer ")
claims = verifier.verify(token)
print(claims["user_id"], claims["tenant_id"], claims["run_id"])
```

`issuer` is required, deliberately with no default: without it, a validly *signed* token from an unrelated tenant sharing the same JWKS endpoint would still pass verification. Pass the issuer your gateway actually issues tokens for.

If you've restricted outbound hosts with `NOVAOS_ALLOWED_HOSTS` (a comma-separated list of allowed hostname suffixes), `gateway_url` is checked against it before any request is made.

## Where these compose

| You are... | Use |
|---|---|
| Calling another agent, with a static credential | `Client(auth=BearerTokenAuth(...))` or a service `APIKeyAuth` |
| Calling another agent, with a credential that refreshes mid-session | `Client(interceptors=[AuthInterceptor(credential_service=...)])` |
| A frontend driving the Execution Gateway | `GatewayClient(auth=BearerTokenAuth(token=novaos_iam_token))` |
| An agent skill making its own downstream call | `ctx.to_execution_context().to_auth_headers()` |
| An agent skill reading a platform-delivered secret | `ctx.get_secret("NAME")` |
| An agent verifying a caller's token itself | `JWKSTokenVerifier` |

See [Error handling](error-handling.md) for how auth failures surface, and the [A2A primer](../concepts/a2a-primer.md) for what an Agent Card's `securitySchemes` actually declare.
