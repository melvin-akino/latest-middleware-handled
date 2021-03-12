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
            
            $unmatchedEvents = Event::getAllUnmatchedEvents($connection);
            if ($connection->numRows($unmatchedEvents)) 
            {
                $events = $connection->fetchAll($unmatchedEvents);
                foreach($events as $event) 
                {
                    //query the leagues part first if this leagueId is existing in the raw table, if not, skip
                    $league = League::getLeague($connection, $event['league_id']);
                    if (!$connection->numRows($league)) 
                    {
                        logger('info', 'matching', "League does not exist", $event['league_id']);
                        continue;
                    }

                    //query league_group if this is already matched
                    $leagueGroupResult = LeagueGroup::checkIfMatched($connection, $event['league_id']);
                    if (!$connection->numRows($leagueGroupResult))
                    {
                        logger('info', 'matching', "League is still not matched", $event['league_id']);
                        continue;
                    }
                    $leagueGroup = $connection->fetchAssoc($leagueGroupResult);
                    $masterLeagueId = $leagueGroup['master_league_id'];

                    //query the teams part second if these teamIds exist in the raw table teams, if not, skip
                    $homeTeamResult = Team::getTeam($connection, $event['home_team_id']);
                    if (!$connection->numRows($homeTeamResult))
                    {
                        logger('info', 'matching', "Home team does not exist", $event['home_team_id']);
                        continue;
                    }

                    $awayTeamResult = Team::getTeam($connection, $event['away_team_id']);
                    if (!$connection->numRows($homeTeamResult))
                    {
                        logger('info', 'matching', "Away team does not exist", $event['away_team_id']);
                        continue;
                    }

                    //query team_group if these teams are already matched
                    $isHomeMatchedResult = TeamGroup::checkIfMatched($connection, $event['home_team_id']);
                    if (!$connection->numRows($isHomeMatchedResult))
                    {
                        logger('info', 'matching', "Home team is still not matched", $event['home_team_id']);
                        continue;
                    }
                    $isHomeMatched = $connection->fetchAssoc($isHomeMatchedResult);
                    $masterHomeTeamId = $isHomeMatched['master_team_id'];

                    $isAwayMatchedResult = TeamGroup::checkIfMatched($connection, $event['away_team_id']);
                    if (!$connection->numRows($isAwayMatchedResult))
                    {
                        logger('info', 'matching', "Away team is still not matched", $event['away_team_id']);
                        continue;
                    }
                    $isAwayMatched = $connection->fetchAssoc($isAwayMatchedResult);
                    $masterAwayTeamId = $isAwayMatched['master_team_id'];                   
                    
                    //if all conditions above are true, continue the verification process to get the master_event_id
                    //Concern starts here - @alex
                    $masterEventResult = MasterEvent::getMasterEventId($connection, $masterLeagueId, $masterHomeTeamId, $masterAwayTeamId);

                    if ($connection->numRows($masterEventResult))
                    {
                        $masterEvent = $connection->fetchAssoc($masterEventResult);
                        //compare if matched master(leagueId, hometeamId, awayteamId) == unmatched master (leagueId, hometeamId, awayteamId)
                        //where to compare these 3 params if not getting from master_events table
                        //please let me know what you think here
                        $matchedEventsResult = EventGroup::getEventByMasterEventId($connection, $masterEvent['id']);
                        if ($connection->numRows($matchedEventsResult)) 
                        {
                            $matchedEvent = $connection->fetchAssoc($matchedEventsResult);
                            if ($matchedEvent['game_schedule'] == $event['game_schedule'])
                            {
                                //create a new record in the pivot table event_group
                                $eventGroup = [
                                    'master_event_id'   => $masterEvent['id'],
                                    'event_id'          => $event['id']
                                ];
                                $result = EventGroup::create($connection, $eventGroup);
                            }
                        }
                    }
                }
            }
            else {
                logger('info', 'matching', "Event remained unmatched", $unmatchedEvents);
                continue;    
            }

            var_dump($result);
            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
