from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

class TeamData(BaseModel):
    idTeam: Optional[int]
    strTeam: Optional[str]
    strTeamShort: Optional[str]
    intFormedYear: Optional[int]
    strSport: Optional[str]
    strLeague: Optional[str]
    idLeague: Optional[int]
    strStadium: Optional[str]
    strLocation: Optional[str]
    intStadiumCapacity: Optional[int]
    strLogo: Optional[str]
    timestamp: datetime

class TeamDataResponse(BaseModel):
    teams: List[TeamData]
    count: int
    message: str



class PlayerData(BaseModel):
    idPlayer: Optional[int]
    idTeam: Optional[int]
    strNationality: Optional[str]
    strPlayer: Optional[str]
    strTeam: Optional[str]
    strSport: Optional[str]
    dateBorn: datetime
    strNumber: Optional[str]
    strWage: Optional[str]
    strBirthLocation: Optional[str]
    strStatus: Optional[str]
    strHeight: Optional[str]
    strWeight: Optional[str]
    strThumb: Optional[str]
    timestamp: datetime

class PlayerDataResponse(BaseModel):
    players: List[PlayerData]
    count: int
    message: str