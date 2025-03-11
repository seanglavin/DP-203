from typing import List, Dict, Optional
from pydantic import BaseModel
from datetime import datetime

class TeamData(BaseModel):
    idTeam: Optional[str]
    strTeam: Optional[str]
    strTeamShort: Optional[str]
    intFormedYear: Optional[str]
    strSport: Optional[str]
    strLeague: Optional[str]
    idLeague: Optional[str]
    strStadium: Optional[str]
    strLocation: Optional[str]
    intStadiumCapacity: Optional[str]
    strLogo: Optional[str]
    timestamp: datetime

class TeamDataResponse(BaseModel):
    teams: List[TeamData]
    count: int
    message: str