import yaml
from pathlib import Path

class ConfigLoader:
    def __init__(self, config_path: str | Path):
        self.config_path = config_path.absolute() if isinstance(config_path, Path) else Path(config_path).absolute()
        with open(self.config_path, 'r') as file:
            self.config = yaml.safe_load(file)
            
    def get(self, key: str):
        return self.config.get(key)