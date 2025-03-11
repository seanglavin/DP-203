from fastapi import APIRouter, HTTPException, Query
from app.models.thesportsdb import TeamData, TeamDataResponse
import logging
from app.services.data_fetcher import extract_team_data, extract_all_nba_teams, extract_all_nhl_teams
from typing import List

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/", tags=["sports"])
async def sports_root():
    return {"message": "Sports data API"}


@router.get("/teams", response_model=TeamDataResponse, tags=["sports"])
async def get_team_data(team_names: List[str] = Query(..., description="List of team names to fetch data for")):
    try:
        logger.info(f"Received request for teams: {team_names}")
        
        # Call the extract_team_data function from data_fetcher.py
        df = extract_team_data(team_names)
        
        if df.empty:
            return TeamDataResponse(
                teams=[],
                count=0,
                message="No team data found"
            )
        
        # Convert DataFrame to list of dictionaries
        teams_data = df.to_dict('records')
        
        # Convert to Pydantic models
        teams = [TeamData(**team) for team in teams_data]
        
        return TeamDataResponse(
            teams=teams,
            count=len(teams),
            message=f"Successfully retrieved data for {len(teams)} teams"
        )
    
    except Exception as e:
        logger.error(f"Error fetching team data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching team data: {str(e)}")


@router.get("/nba/teams", response_model=TeamDataResponse, tags=["sports"])
async def get_all_nba_teams():
    """
    Fetches data for all NBA teams
    """
    try:
        logger.info("Received request for all NBA teams")
        
        # Call the extract_all_nba_teams function from data_fetcher.py
        df = extract_all_nba_teams()
        
        if df.empty:
            return TeamDataResponse(
                teams=[],
                count=0,
                message="No NBA team data found"
            )
        
        # Convert DataFrame to list of dictionaries
        teams_data = df.to_dict('records')
        
        # Convert to Pydantic models
        teams = [TeamData(**team) for team in teams_data]
        
        return TeamDataResponse(
            teams=teams,
            count=len(teams),
            message=f"Successfully retrieved data for {len(teams)} NBA teams"
        )
    
    except Exception as e:
        logger.error(f"Error fetching NBA team data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching NBA team data: {str(e)}")


@router.get("/nhl/teams", response_model=TeamDataResponse, tags=["sports"])
async def get_all_nhl_teams():
    """
    Fetches data for all NHL teams
    """
    try:
        logger.info("Received request for all NHL teams")
        
        # Call the extract_all_nhl_teams function from data_fetcher.py
        df = extract_all_nhl_teams()
        
        if df.empty:
            return TeamDataResponse(
                teams=[],
                count=0,
                message="No NHL team data found"
            )
        
        # Convert DataFrame to list of dictionaries
        teams_data = df.to_dict('records')
        
        # Convert to Pydantic models
        teams = [TeamData(**team) for team in teams_data]
        
        return TeamDataResponse(
            teams=teams,
            count=len(teams),
            message=f"Successfully retrieved data for {len(teams)} NHL teams"
        )
    
    except Exception as e:
        logger.error(f"Error fetching NHL team data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching NHL team data: {str(e)}")