import pandas as pd
import requests
from fastapi import APIRouter, Depends, HTTPException
from app.models.thesportsdb import TeamData, TeamDataResponse, PlayerData, PlayerDataResponse
from app.services.data_fetcher import (
    extract_all_nba_teams, 
    extract_all_nhl_teams, 
    extract_team_details, 
    extract_team_players, 
    extract_player_details
)
from app.services.get_storage_client import get_storage_client
from app.config_settings import settings
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

#
@router.post("/nhl/players/all", response_model=PlayerDataResponse)
async def save_all_nhl_players(storage_client=Depends(get_storage)):
    """
    Save player data for ALL NHL teams to Azure Storage
    """
    try:
        logger.info("Saving all NHL players...")

        # Get all NHL teams first
        response = requests.get(
            f"{settings.THESPORTSDB_FREE_API_BASE}/search_all_teams.php?l=NHL",
            timeout=5
        )
        
        if not response.ok:
            logger.error(f"API request failed for NHL teams. Status code: {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()
        
        # Get list of teams from the response
        teams_response = data.get("teams", [])
        # Extract just the "idTeam" values
        team_ids_list = [team.get("idTeam") for team in teams_response]
        # If you want to convert them to integers:
        team_ids_list_int = [int(team_id) for team_id in team_ids_list]

        logger.info(f"Found {len(team_ids_list)} teams")
        logger.info(f"team_ids_list: {team_ids_list}")

        # Initialize empty DataFrame to collect all players
        all_players_df = pd.DataFrame()

        # Process each team ID
        for team_id in team_ids_list_int:
            # Get players for this team
            team_players = extract_team_players(team_id)

            if not team_players.empty:
                # Append to all_players_df
                all_players_df = pd.concat([all_players_df, team_players], ignore_index=True)

        if not all_players_df.empty:
            # Upload combined player data
            blob_name = "nhl/players.csv"
            success = await storage_client.upload_dataframe(all_players_df, blob_name)

            return {
                "players": all_players_df.to_dict("records"),
                "count": len(all_players_df),
                "message": f"Successfully saved ALL NHL players - {success}"
            }
        else:
            return {"players": [], "count": 0, "message": "No NHL players found"}

    except Exception as e:
        logger.error(f"Error saving NHL players: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
#
@router.post("/nba/players/all", response_model=PlayerDataResponse)
async def save_all_nba_players(storage_client=Depends(get_storage)):
    """
    Save player data for ALL NBA teams to Azure Storage
    """
    try:
        logger.info("Saving all NBA players...")

        # Get all NBA teams first
        nba_teams = extract_all_nba_teams()

        if not nba_teams.empty:
            # The response contains a "teams" column with team objects
            # We need to extract the idTeam from each object in that column
            team_ids = []

            for _, row in nba_teams.iterrows():
                if isinstance(row["teams"], list):
                    for team in row["teams"]:
                        team_id = team.get("idTeam")
                        if team_id:
                            team_ids.append(team_id)

            logger.info(f"Found {len(team_ids)} teams")

            # Initialize empty DataFrame to collect all players
            all_players_df = pd.DataFrame()

            # Process each team ID
            for team_id in team_ids:

                # Get players for this team
                team_players = extract_team_players(team_id)

                if not team_players.empty:
                    # Append to all_players_df
                    all_players_df = pd.concat([all_players_df, team_players], ignore_index=True)

            # Upload combined player data
            blob_name = "nba/players.csv"
            success = await storage_client.upload_dataframe(all_players_df, blob_name)

            return {
                "players": all_players_df.to_dict("records"),
                "count": len(all_players_df),
                "message": f"Successfully saved ALL NBA players - {success}"
            }

        return {"players": [], "count": 0, "message": "No NBA players found"}

    except Exception as e:
        logger.error(f"Error saving NBA players: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))



# @router.get("/teams/{team_id}/players", response_model=List[TeamData])
# async def get_team_players(team_id: int, storage_client=Depends(get_storage)):
#     """
#     Get all players for a specific team by ID from TheSportsDB API
#     """
#     try:
#         logger.info(f"Fetching players for team ID: {team_id}")

#         df = extract_team_players(team_id)

#         if not df.empty:
#             return df.to_dict("records")

#         return []

#     except Exception as e:
#         logger.error(f"Error fetching team players: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))


# @router.get("/team/{team_id}/details", response_model=TeamData)
# async def get_team_details(team_id: int, storage_client=Depends(get_storage)):
#     """
#     Get detailed information for a specific team by ID from TheSportsDB API
#     """
#     try:
#         logger.info(f"Fetching details for team ID: {team_id}")

#         df = extract_team_details(team_id)

#         if not df.empty:
#             return df.to_dict("records")[0]

#         raise HTTPException(status_code=404, detail=f"Team with ID {team_id} not found")

#     except Exception as e:
#         logger.error(f"Error fetching team details: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))


# @router.get("/players/{player_id}", response_model=TeamData)
# async def get_player_details(player_id: int, storage_client=Depends(get_storage)):
#     """
#     Get detailed information for a specific player by ID from TheSportsDB API
#     """
#     try:
#         logger.info(f"Fetching details for player ID: {player_id}")

#         df = extract_player_details(player_id)

#         if not df.empty:
#             return df.to_dict("records")[0]

#         raise HTTPException(status_code=404, detail=f"Player with ID {player_id} not found")

#     except Exception as e:
#         logger.error(f"Error fetching player details: {str(e)}")
#         raise HTTPException(status_code=500, detail=str(e))

