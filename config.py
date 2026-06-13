from dataclasses import dataclass

@dataclass
class Config:
    nickname: str
    token_data_time: float
    error_probability: float
    token_timeout: float
    min_token_interval: float

def load_config(path: str) -> Config:
    with open(path, 'r') as f:
        lines = f.read().splitlines()
        return Config(
            nickname=lines[0].strip(),
            token_data_time=float(lines[1].strip().replace(',', '.')),
            error_probability=float(lines[2].strip().replace(',', '.')),
            token_timeout=float(lines[3].strip().replace(',', '.')),
            min_token_interval=float(lines[4].strip().replace(',', '.'))
        )