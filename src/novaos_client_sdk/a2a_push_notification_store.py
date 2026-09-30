"""``TaskPushNotificationConfig`` persistence for the NovaOS client SDK.

Mirrors :mod:`novaos_client_sdk.a2a_task_store` deliberately closely: same
ABC/in-memory split, same ``asyncio.Lock``-guarded ``OrderedDict``, same
LRU-by-write eviction policy. The client side of A2A's push notification
config methods needed somewhere to send a
:class:`~novaos_client_sdk.a2a_push_notification.TaskPushNotificationConfig`
*to*; this is where an agent pod holds one once it arrives, so
``CreateTaskPushNotificationConfig`` has something for
``GetTaskPushNotificationConfig`` / ``ListTaskPushNotificationConfigs`` /
``DeleteTaskPushNotificationConfig`` to read back.

**Keyed by ``(task_id, config_id)``, not by ``config_id`` alone.** A2A allows
more than one registration per task (spec §3.1.7) — that is the entire reason
``ListTaskPushNotificationConfigs`` exists rather than a caller remembering the
one config it set — so a config's identity on this side is the pair, matching
what every one of the four wire methods actually addresses a config by
(:class:`~novaos_client_sdk.client.Client`'s ``get_task_push_notification_config``
and ``delete_task_push_notification_config`` both take ``task_id`` *and*
``config_id``).

**Scope, same as the task store (ADR F9/F15).** An in-process store is the
right default for the same reason ``InMemoryTaskStore`` is: a pod is not
internet-reachable, and there is no gateway-owned durable record for push
notification config the way there is for run status — so there is nothing for
a cross-pod-durable implementation to compete with. A pod that wants push
notification config to survive a restart injects its own
:class:`PushNotificationConfigStore` implementation; this SDK ships only the
in-memory one, matching the task store's own single-implementation posture.

**This module records; it does not deliver.** A config saved
here is held for the platform to act on — nothing in this SDK ever dials the
callback ``url`` a config carries. Delivering the actual webhook, where a
deployment does that at all, is the platform gateway's job, not
this store's or this agent's. A caller-supplied ``url`` is still validated
against the same SSRF guard used for agent discovery
(:func:`~novaos_client_sdk.dns.validate_agent_url`) before ``Create`` ever
reaches :meth:`PushNotificationConfigStore.save` — see
:meth:`~novaos_client_sdk.agent_builder.AgentBuilder._dispatch_push_notification_config` —
so a config resolving to a private, loopback or link-local address is
rejected at registration rather than merely never dialled.
"""

import asyncio
from abc import ABC, abstractmethod
from collections import OrderedDict

from .a2a_push_notification import TaskPushNotificationConfig

__all__ = [
    "DEFAULT_MAX_PUSH_CONFIGS",
    "InMemoryPushNotificationConfigStore",
    "PushNotificationConfigStore",
]

#: Default cap for :class:`InMemoryPushNotificationConfigStore`, sized the same
#: as :data:`~novaos_client_sdk.a2a_task_store.DEFAULT_MAX_TASKS` and for the
#: same reason: a fleet-mode pod kept warm across many runs must not retain
#: every registration it has ever seen.
DEFAULT_MAX_PUSH_CONFIGS = 1000


class PushNotificationConfigStore(ABC):
    """Where a :class:`~novaos_client_sdk.a2a_push_notification.TaskPushNotificationConfig` is kept once it exists.

    Four operations, matching what
    :class:`~novaos_client_sdk.agent_builder.AgentBuilder`'s four
    ``*TaskPushNotificationConfig`` handlers actually do to a registration:
    write it, read one back by ``(task_id, config_id)``, list every
    registration for a task, and remove one. An ABC for the same reason
    :class:`~novaos_client_sdk.a2a_task_store.TaskStore` is: a subclass backing
    this with something other than memory implements the same four methods and
    ``AgentBuilder`` does not need to know the difference.
    """

    @abstractmethod
    async def save(self, config: TaskPushNotificationConfig) -> None:
        """Persist ``config``, replacing any earlier config with the same
        ``(task_id, id)``."""

    @abstractmethod
    async def get(self, task_id: str, config_id: str) -> TaskPushNotificationConfig | None:
        """Return the config stored under ``(task_id, config_id)``, or ``None``."""

    @abstractmethod
    async def list(self, task_id: str) -> list[TaskPushNotificationConfig]:
        """Return every config registered against ``task_id``, in store order.

        No pagination here — that is the JSON-RPC handler's job
        (``ListTaskPushNotificationConfigs``), the same store/handler division
        of labour :meth:`~novaos_client_sdk.a2a_task_store.TaskStore.list_tasks`
        draws for tasks.
        """

    @abstractmethod
    async def delete(self, task_id: str, config_id: str) -> None:
        """Remove the config stored under ``(task_id, config_id)``. A no-op if there is none."""


class InMemoryPushNotificationConfigStore(PushNotificationConfigStore):
    """A :class:`PushNotificationConfigStore` backed by a dict in this process's memory.

    The default (and, per ADR F9/F15, the only) store this SDK ships — see the
    module docstring.

    **Eviction policy.** Identical to
    :class:`~novaos_client_sdk.a2a_task_store.InMemoryTaskStore`'s: bounded by
    ``max_configs`` (default :data:`DEFAULT_MAX_PUSH_CONFIGS`), tracked in
    least-recently-saved order. Every :meth:`save` moves that ``(task_id,
    config_id)`` entry to the most-recently-saved end; once the store holds
    more than ``max_configs`` entries, the least-recently-saved one is evicted.
    A straight LRU-by-write policy, not a TTL — no background sweep, eviction
    only ever happens as a side effect of a new :meth:`save`.
    """

    def __init__(self, max_configs: int = DEFAULT_MAX_PUSH_CONFIGS) -> None:
        """Initialize the store.

        Args:
            max_configs: The eviction cap described above. Must be positive.
        """
        if max_configs <= 0:
            raise ValueError("max_configs must be greater than 0")
        self._max_configs = max_configs
        self._configs: OrderedDict[tuple[str, str], TaskPushNotificationConfig] = OrderedDict()
        self._lock = asyncio.Lock()

    async def save(self, config: TaskPushNotificationConfig) -> None:
        """Persist ``config``, evicting the least-recently-saved entry if over capacity."""
        key = (config.task_id, config.id)
        async with self._lock:
            self._configs[key] = config
            self._configs.move_to_end(key)
            if len(self._configs) > self._max_configs:
                self._configs.popitem(last=False)

    async def get(self, task_id: str, config_id: str) -> TaskPushNotificationConfig | None:
        """Return the config stored under ``(task_id, config_id)``, or ``None``.

        Does **not** count as a "save" for eviction-ordering purposes, matching
        :meth:`~novaos_client_sdk.a2a_task_store.InMemoryTaskStore.get`.
        """
        async with self._lock:
            return self._configs.get((task_id, config_id))

    async def list(self, task_id: str) -> list[TaskPushNotificationConfig]:
        """Return every config registered against ``task_id``, in store order.

        Scoped strictly to ``task_id`` — a config registered against a
        different task must never leak into this task's page, which is the
        entire reason the store key is the ``(task_id, config_id)`` pair
        rather than ``config_id`` alone.
        """
        async with self._lock:
            return [config for (tid, _cid), config in self._configs.items() if tid == task_id]

    async def delete(self, task_id: str, config_id: str) -> None:
        """Remove the config stored under ``(task_id, config_id)``. A no-op if there is none."""
        async with self._lock:
            self._configs.pop((task_id, config_id), None)
