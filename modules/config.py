from dataclasses import dataclass

@dataclass
class Config:
    server = '127.0.0.1'
    user = 'sa'
    passwd = 'triska'
    database = 'PVestmed'