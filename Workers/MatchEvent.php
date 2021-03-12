<?php

namespace Workers;

use Models\{
    Event,
    EventGroup,    
    League,
    LeagueGroup,
    Team,
    TeamGroup,
    MasterEvent
};
use Co\System;
use Exception;

class MatchEvent
{
    public static function handle($dbPool, $event) 
    {
        while (true)
        {
            $connection = $dbPool->borrow();
            
            //query the leagues part first if this leagueId is existing in the raw table, if not, skip
            $league = League::getLeague($connection, $event['league_id']);
            if (empty($connection->fetchAssoc($league)) 
            {
                logger('info', 'matching', "League does not exist", $event['league_id']);
                continue;
            }

            //query league_group if this is already matched
            $leagueGroup = LeagueGroup::checkIfMatched($connection, $event['league_id']);
            if (empty($connection->fetchAssoc($leagueGroup))
            {
                logger('info', 'matching', "League is still not matched", $event['league_id']);
                continue;
            }
            $masterLeagueId = $leagueGroup['master_league_id'];

            //query the teams part second if these teamIds exist in the raw table teams, if not, skip
            $homeTeam = Team::getTeam($connection, $event['home_team_id']);
            if (empty($connection->fetchAssoc($homeTeam))
            {
                logger('info', 'matching', "Home team does not exist", $event['home_team_id']);
                continue;
            }

            $awayTeam = Team::getTeam($connection, $event['away_team_id']);
            if (empty($connection->fetchAssoc($awayTeam))
            {
                logger('info', 'matching', "Away team does not exist", $event['away_team_id']);
                continue;
            }

            //query team_group if these teams are already matched
            $isHomeMatched = TeamGroup::checkIfMatched($connection, $event['home_team_id']);
            if (empty($connection->fetchAssoc($isHomeMatched))
            {
                logger('info', 'matching', "Home team is still not matched", $event['home_team_id']);
                continue;
            }
            $masterHomeTeamId = $isHomeMatched['master_team_id'];

            $isAwayMatched = TeamGroup::checkIfMatched($connection, $event['away_team_id']);
            if (empty($connection->fetchAssoc($isAwayMatched))
            {
                logger('info', 'matching', "Away team is still not matched", $event['away_team_id']);
                continue;
            }
            $masterAwayTeamId = $isAwayMatched['master_team_id'];
            
            
            //if all conditions above are true, continue the verification process to get the master_event_id
            //compare if matched master(leagueId, hometeamId, awayteamId) == unmatched master (leagueId, hometeamId, awayteamId)
            $masterEvent = MasterEvent::checkIfIdExists($connection, $masterLeagueId, $masterHomeTeamId, $masterAwayTeamId);

            if (!empty($masterEvent))
            {
                //create a new record in the pivot table event_group
                $eventGroup = [
                    'master_event_id'   => $masterEvent['id'],
                    'event_id'          => $event['id']
                ];
                $result = EventGroup::matchEvent($connection, $eventGroup);
            }

            var_dump($result);
            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
