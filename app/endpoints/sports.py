from fastapi import APIRouter, Depends, HTTPException
from app.models.thesportsdb import TeamData, TeamDataResponse
from app.services.data_fetcher import (
    extract_all_nba_teams, 
    extract_all_nhl_teams, 
    extract_team_details, 
    extract_team_players, 
    extract_player_details
)
from app.services.get_storage_client import get_storage_client
from datetime import datetime
from logger_config import logger
from typing import List

router = APIRouter()


# Use dependency injection for the storage client
async def get_storage():
    """Dependency to get storage client"""
    return get_storage_client()



@router.get("/", tags=["sports"])
async def sports_root():
    return {"message": "Sports data API"}

#
@router.get("/nba/teams", response_model=TeamDataResponse)
async def read_nba_teams(storage_client=Depends(get_storage)):
    """Get all NBA teams from TheSportsDB API"""
    try:
        df = extract_all_nba_teams()
        if not df.empty:
            return {"teams": df.to_dict("records"), "count": len(df), "message": "Successfully fetched NBA teams"}
        return {"teams": [], "count": 0, "message": "No NBA teams found"}
    except Exception as e:
        logger.error(f"Error fetching NBA teams: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
# 
@router.get("/nhl/teams", response_model=TeamDataResponse)
async def read_nhl_teams(storage_client=Depends(get_storage)):
    """Get all NHL teams from TheSportsDB API"""
    try:
        df = extract_all_nhl_teams()
        if not df.empty:
            return {"teams": df.to_dict("records"), "count": len(df), "message": "Successfully fetched NHL teams"}
        return {"teams": [], "count": 0, "message": "No NHL teams found"}
    except Exception as e:
        logger.error(f"Error fetching NHL teams: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

#
@router.post("/nba/teams", response_model=TeamDataResponse)
async def save_nba_teams(storage_client=Depends(get_storage)):
    """Save NBA team data to Azure Storage"""
    try:
        df = extract_all_nba_teams()
        if not df.empty:
            success = await storage_client.upload_dataframe(df, "nba/teams.csv")
            message = f"Successfully saved NBA teams - {success}"
            return {"teams": df.to_dict("records"), "count": len(df), "message": message}
        return {"teams": [], "count": 0, "message": "No NBA teams found to save"}
    except Exception as e:
        logger.error(f"Error saving NBA teams: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

#
@router.post("/nhl/teams", response_model=TeamDataResponse)
async def save_nhl_teams(storage_client=Depends(get_storage)):
    """Save NHL team data to Azure Storage"""
    try:
        df = extract_all_nhl_teams()
        if not df.empty:
            success = await storage_client.upload_dataframe(df, "nhl/teams.csv")
            message = f"Successfully saved NHL teams - {success}"
            return {"teams": df.to_dict("records"), "count": len(df), "message": message}
        return {"teams": [], "count": 0, "message": "No NHL teams found to save"}
    except Exception as e:
        logger.error(f"Error saving NHL teams: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))




@router.get("/teams/{team_id}/players", response_model=List[TeamData])
async def get_team_players(team_id: int, storage_client=Depends(get_storage)):
    """
    Get all players for a specific team by ID from TheSportsDB API
    """
    try:
        logger.info(f"Fetching players for team ID: {team_id}")

        df = extract_team_players(team_id)

        if not df.empty:
            return df.to_dict("records")

        return []

    except Exception as e:
        logger.error(f"Error fetching team players: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/team/{team_id}/details", response_model=TeamData)
async def get_team_details(team_id: int, storage_client=Depends(get_storage)):
    """
    Get detailed information for a specific team by ID from TheSportsDB API
    """
    try:
        logger.info(f"Fetching details for team ID: {team_id}")

        df = extract_team_details(team_id)

        if not df.empty:
            return df.to_dict("records")[0]

        raise HTTPException(status_code=404, detail=f"Team with ID {team_id} not found")

    except Exception as e:
        logger.error(f"Error fetching team details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/players/{player_id}", response_model=TeamData)
async def get_player_details(player_id: int, storage_client=Depends(get_storage)):
    """
    Get detailed information for a specific player by ID from TheSportsDB API
    """
    try:
        logger.info(f"Fetching details for player ID: {player_id}")

        df = extract_player_details(player_id)

        if not df.empty:
            return df.to_dict("records")[0]

        raise HTTPException(status_code=404, detail=f"Player with ID {player_id} not found")

    except Exception as e:
        logger.error(f"Error fetching player details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

