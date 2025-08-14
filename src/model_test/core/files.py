from pathlib import Path
from jinja2 import Environment, BaseLoader
from typing import Dict, Optional

class LocalFile:

    def __init__(
        self,
        path: str,
        options: Optional[Dict[str,str]] = {}
    ):
        self.path = Path(path)
        self.content = self.path.read_text()
        self.format = self.path.suffix.lstrip(".").lower()
        self.options = options

class SQLFile(LocalFile):

    def __init__(
        self,
        path: str,
        options: Optional[Dict[str,str]] = {}
    ):
        super().__init__(path, options)

    def render(self, staged_map: Dict[str, str]) -> str:
        env = Environment(loader=BaseLoader(), autoescape=False, trim_blocks=True, lstrip_blocks=True)

        # Disponibiliza helpers dependentes do backend
        env.globals["ref"] = lambda name: staged_map[name]                      # {{ ref('orders') }}
        # env.filters["ts"] = self.adapter.lit_timestamp                        # {{ '2025-01-01'|ts }}
        # env.filters["d"]  = self.adapter.lit_date                             # {{ '2025-01-01'|d }}

        return env.from_string(self.content).render(params=self.options)
