"""Session state management for spark-history-cli.

Maintains the current server URL and active app context across REPL commands.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict


@dataclass
class Session:
    """Tracks REPL session state."""

    server_url: str = "http://localhost:18080"
    current_app_id: str | None = None
    current_attempt_id: str | None = None

    _path: str | None = field(default=None, repr=False)

    def set_app(self, app_id: str, attempt_id: str | None = None) -> None:
        """Set the current application context."""
        self.current_app_id = app_id
        self.current_attempt_id = attempt_id

    def clear_app(self) -> None:
        """Clear the current application context."""
        self.current_app_id = None
        self.current_attempt_id = None

    def require_app(self) -> str:
        """Return current app ID or raise an error if none is set."""
        if not self.current_app_id:
            raise ValueError(
                "No application selected. Use 'use <app-id>' or pass --app-id."
            )
        return self.current_app_id

    @property
    def context_label(self) -> str:
        """Return a short label for the current context (for prompt display)."""
        if not self.current_app_id:
            return ""
        label = self.current_app_id
        if len(label) > 30:
            label = label[:12] + "..." + label[-12:]
        if self.current_attempt_id:
            label += f"/{self.current_attempt_id}"
        return label

    def save(self, path: str | None = None) -> None:
        """Save session state to a JSON file."""
        path = path or self._path
        if not path:
            return
        data = {
            "server_url": self.server_url,
            "current_app_id": self.current_app_id,
            "current_attempt_id": self.current_attempt_id,
        }
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    @classmethod
    def load(cls, path: str) -> Session:
        """Load session state from a JSON file."""
        try:
            with open(path) as f:
                data = json.load(f)
            session = cls(
                server_url=data.get("server_url", "http://localhost:18080"),
                current_app_id=data.get("current_app_id"),
                current_attempt_id=data.get("current_attempt_id"),
            )
            session._path = path
            return session
        except (FileNotFoundError, json.JSONDecodeError):
            session = cls()
            session._path = path
            return session
