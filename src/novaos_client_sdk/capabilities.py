"""Capability matching and indexing utilities for NovaOS Client SDK."""

from dataclasses import dataclass

from novaos_client_sdk.dns import AgentDNSRecord
from novaos_client_sdk.utils import match_capabilities, normalize_capability_name


@dataclass
class MatchResult:
    """Result of capability matching."""

    agent: AgentDNSRecord
    matched_capabilities: list[str]
    missing_capabilities: list[str]
    score: float

    def __post_init__(self) -> None:
        """Validate match result."""
        if not 0.0 <= self.score <= 1.0:
            raise ValueError(f"Score must be between 0.0 and 1.0, got {self.score}")


class CapabilityIndex:
    """Index for fast capability-based agent lookup."""

    def __init__(self, agents: list[AgentDNSRecord]) -> None:
        """Build index from agents.

        Args:
            agents: List of agents to index
        """
        self.agents = agents
        self._capability_map: dict[str, list[AgentDNSRecord]] = {}
        self._build_index()

    def _build_index(self) -> None:
        """Build capability-to-agents mapping."""
        for agent in self.agents:
            # Index by capabilities from simple list
            for cap_name in agent.capabilities:
                normalized = normalize_capability_name(cap_name)
                if normalized not in self._capability_map:
                    self._capability_map[normalized] = []
                if agent not in self._capability_map[normalized]:
                    self._capability_map[normalized].append(agent)

            # Index by capabilities from metadata
            parsed_caps = agent.get_capabilities()
            for cap in parsed_caps:
                normalized = normalize_capability_name(cap.name)
                if normalized not in self._capability_map:
                    self._capability_map[normalized] = []
                if agent not in self._capability_map[normalized]:
                    self._capability_map[normalized].append(agent)

    def find_agents_by_capability(
        self,
        capability: str,
        fuzzy: bool = False,
    ) -> list[AgentDNSRecord]:
        """Find agents with specific capability.

        Args:
            capability: Capability name to search for
            fuzzy: If True, use fuzzy matching

        Returns:
            List of agents with the capability
        """
        if not capability:
            return []

        normalized = normalize_capability_name(capability)
        agents = self._capability_map.get(normalized, [])

        if fuzzy and not agents:
            # Try fuzzy matching
            for norm_cap, agent_list in self._capability_map.items():
                match = match_capabilities(capability, [norm_cap], threshold=0.8)
                if match:
                    agents.extend(agent_list)

        # Remove duplicates while preserving order
        seen = set()
        unique_agents = []
        for agent in agents:
            if agent.id not in seen:
                seen.add(agent.id)
                unique_agents.append(agent)

        return unique_agents

    def find_agents_by_capabilities(
        self,
        capabilities: list[str],
        require_all: bool = True,
        fuzzy: bool = False,
    ) -> list[AgentDNSRecord]:
        """Find agents matching multiple capabilities.

        Args:
            capabilities: List of required capability names
            require_all: If True, agent must have all capabilities (AND).
                        If False, agent needs at least one (OR)
            fuzzy: If True, use fuzzy matching

        Returns:
            List of agents matching the criteria
        """
        if not capabilities:
            return []

        if require_all:
            # Agent must have all capabilities
            agent_id_sets: list[set[str]] = []
            for cap in capabilities:
                agents = self.find_agents_by_capability(cap, fuzzy=fuzzy)
                agent_id_sets.append({agent.id for agent in agents})

            if not agent_id_sets:
                return []

            # Find intersection of all sets (agent IDs)
            result_ids = agent_id_sets[0]
            for agent_id_set in agent_id_sets[1:]:
                result_ids = result_ids & agent_id_set

            # Convert back to agents
            agent_map = {agent.id: agent for agent in self.agents}
            return [agent_map[agent_id] for agent_id in result_ids if agent_id in agent_map]
        else:
            # Agent needs at least one capability
            all_agent_ids: set[str] = set()
            for cap in capabilities:
                agents = self.find_agents_by_capability(cap, fuzzy=fuzzy)
                all_agent_ids.update(agent.id for agent in agents)

            # Convert back to agents
            agent_map = {agent.id: agent for agent in self.agents}
            return [agent_map[agent_id] for agent_id in all_agent_ids if agent_id in agent_map]


class CapabilityMatcher:
    """Service for matching capabilities to agents."""

    def __init__(
        self,
        similarity_threshold: float = 0.8,
        normalize_names: bool = True,
    ) -> None:
        """Initialize matcher.

        Args:
            similarity_threshold: Minimum similarity score (0.0-1.0) for fuzzy matching
            normalize_names: If True, normalize capability names before matching
        """
        if not 0.0 <= similarity_threshold <= 1.0:
            raise ValueError(
                f"similarity_threshold must be between 0.0 and 1.0, got {similarity_threshold}"
            )
        self.similarity_threshold = similarity_threshold
        self.normalize_names = normalize_names

    def match(
        self,
        agents: list[AgentDNSRecord],
        required_capabilities: list[str],
        match_mode: str = "any",  # "any" or "all"
    ) -> list[MatchResult]:
        """Match agents to required capabilities.

        Args:
            agents: List of agents to match against
            required_capabilities: List of required capability names
            match_mode: "any" for OR logic (agent needs at least one),
                       "all" for AND logic (agent must have all)

        Returns:
            List of MatchResult objects sorted by score (descending)
        """
        if not agents or not required_capabilities:
            return []

        results: list[MatchResult] = []

        for agent in agents:
            matched: list[str] = []
            missing: list[str] = []

            # Get all agent capabilities
            agent_capabilities = agent.get_capabilities()
            agent_cap_names = [cap.name for cap in agent_capabilities]
            # Also include simple string list
            agent_cap_names.extend(agent.capabilities)

            # Match each required capability
            for required_cap in required_capabilities:
                found = False

                # Try exact match first
                for agent_cap in agent_cap_names:
                    if self.normalize_names:
                        if normalize_capability_name(required_cap) == normalize_capability_name(
                            agent_cap
                        ):
                            matched.append(required_cap)
                            found = True
                            break
                    else:
                        if required_cap == agent_cap:
                            matched.append(required_cap)
                            found = True
                            break

                # Try fuzzy match if not found
                if not found:
                    match_result = match_capabilities(
                        required_cap, agent_cap_names, threshold=self.similarity_threshold
                    )
                    if match_result:
                        matched.append(required_cap)
                        found = True

                if not found:
                    missing.append(required_cap)

            # Calculate score
            if match_mode == "all":
                # Agent must have all capabilities
                if not missing:
                    score = 1.0
                else:
                    score = len(matched) / len(required_capabilities)
            else:
                # Agent needs at least one capability
                if matched:
                    score = len(matched) / len(required_capabilities)
                else:
                    score = 0.0

            # Only include if matches criteria
            if match_mode == "all" and missing:
                continue  # Skip if missing any (for "all" mode)
            elif match_mode == "any" and not matched:
                continue  # Skip if no matches (for "any" mode)

            results.append(
                MatchResult(
                    agent=agent,
                    matched_capabilities=matched,
                    missing_capabilities=missing,
                    score=score,
                )
            )

        # Sort by score (descending)
        results.sort(key=lambda x: x.score, reverse=True)

        return results
