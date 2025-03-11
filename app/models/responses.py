from pydantic import BaseModel
from typing import List, Dict, Optional

class SaveResponse(BaseModel):
    """Response model for save operation endpoint"""
    message: str

class TeamDataResponse(BaseModel):
    """Response model for team data endpoint"""
    idTeam: Optional[str] = None
    strTeam: Optional[str] = None
    strTeamShort: Optional[str] = None
    intFormedYear: Optional[str] = None
    strSport: Optional[str] = None
    strLeague: Optional[str] = None
    idLeague: Optional[str] = None
    strStadium: Optional[str] = None
    strLocation: Optional[str] = None
    intStadiumCapacity: Optional[str] = None
    strLogo: Optional[str] = None
    timestamp: Optional[str] = None

# # For importing in other files:
# __all__ = ["SaveResponse", "TeamDataResponse"]