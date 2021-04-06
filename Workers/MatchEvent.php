<?php

namespace Workers;

use Models\{
    Event,
    EventGroup,    
    MasterEvent,
    UnmatchedEvent,
    UnmatchedData,
    Provider,
    TeamGroup
};
use Co\System;
use Exception;
use Carbon\Carbon;

class MatchEvent
{
    public static function handle($dbPool, $swooleTable) 
    {
        while (true)
        {
            $connection = $dbPool->borrow();

            try {
                $primaryProviderId    = Provider::getIdByAlias($connection, $swooleTable['systemConfig']['PRIMARY_PROVIDER']['value']);
                $unmatchedEventResult = Event::getAllUnmatchedEvents($connection);
                $unmatchedEvents      = $connection->fetchAll($unmatchedEventResult);

                foreach($unmatchedEvents as $unmatchedEvent) {
                    if ($unmatchedEvent['provider_id'] == $primaryProviderId && !empty($unmatchedEvent['event_identifier'])) {
                        foreach ($swooleTable['matchedLeagues'] as $ml) {
                            if ($ml['league_id'] == $unmatchedEvent['league_id']) {
                                $masterLeagueId = $ml['master_league_id'];

                                $masterEventUniqueId = implode('-', [
                                    date("Ymd", strtotime($unmatchedEvent['ref_schedule'])),
                                    $unmatchedEvent['sport_id'],
                                    $masterLeagueId,
                                    $unmatchedEvent['event_identifier']
                                ]);

                                $eventHasMasterResult = MasterEvent::checkIfHasMaster($connection, $masterEventUniqueId);

                                if (!$connection->numRows($eventHasMasterResult)) {
                                    $masterTeamHomeResult = TeamGroup::checkIfMatched($connection, $unmatchedEvent['team_home_id']);
                                    if (!$connection->numRows($masterTeamHomeResult)) {
                                        continue 2;
                                    }
                                    $masterTeamHome = $connection->fetchArray($masterTeamHomeResult);

                                    $masterTeamAwayResult = TeamGroup::checkIfMatched($connection, $unmatchedEvent['team_home_id']);
                                    if (!$connection->numRows($masterTeamAwayResult)) {
                                        continue 2;
                                    }

                                    $masterTeamAway = $connection->fetchArray($masterTeamAwayResult);

                                    $masterEventResult = MasterEvent::create($connection, [
                                        'master_event_unique_id' => $masterEventUniqueId,
                                        'master_league_id'       => $masterLeagueId,
                                        'master_team_home_id'    => $masterTeamHome['master_team_id'],
                                        'master_team_away_id'    => $masterTeamAway['master_team_id'],
                                        'sport_id'               => $unmatchedEvent['sport_id'],
                                        'created_at'             => Carbon::now()
                                    ], 'id');

                                    $masterEvent = $connection->fetchArray($masterEventResult);

                                    EventGroup::create($connection, [
                                        'master_event_id' => $masterEvent['id'],
                                        'event_id'        => $unmatchedEvent['id']
                                    ]);
                                } else {
                                    $eventHasMaster = $connection->fetchArray($eventHasMasterResult);
                                    $masterEventId  = $eventHasMaster['id'];
                                    EventGroup::create($connection, [
                                        'master_event_id' => $masterEventId,
                                        'event_id'        => $unmatchedEvent['id']
                                    ]);
                                }

                                $key = implode(':', [
                                    'pId:' . $unmatchedEvent['provider_id'],
                                    'event_identifier:' . $unmatchedEvent['event_identifier'],
                                ]);

                                UnmatchedData::removeToUnmatchedData($connection, [
                                    'data_id'   => $unmatchedEvent['id'],
                                    'data_type' => 'event'
                                ]);
                                $swooleTable['unmatchedEvents']->del($key);
                                continue 2;
                            }
                        }
                    }
                }

                $unmatchedEvents = Event::getAllGroupVerifiedUnmatchedEvents($connection);
                if ($connection->numRows($unmatchedEvents)) 
                {
                    $events = $connection->fetchAll($unmatchedEvents);
                    foreach($events as $event) 
                    {                                     
                        //if all conditions above are true, continue the verification process to get the master_event_id
                        $masterEventResult = MasterEvent::getMasterEventData($connection, $event['master_league_id'], $event['master_home_team_id'], $event['master_away_team_id'], $event['ref_schedule']);

                        //if a master event is returned
                        if ($connection->numRows($masterEventResult))
                        {
                            
                            $masterEvent = $connection->fetchAssoc($masterEventResult);

                            //create a new record in the pivot table event_groups
                            $eventGroup = [
                                'master_event_id'   => $masterEvent['id'],
                                'event_id'          => $event['event_id']
                            ];
                            $eventGroupResult = EventGroup::create($connection, $eventGroup);

                            //Delete it from the unmatched table
                            UnmatchedEvent::deleteUnmatched($connection, $event['event_id']);
                        } else {
                            logger('info', 'matching', "Event remained unmatched", $event);
                            continue;    
                        }
                    }
                }
            } catch (Exception $e) {
                logger('error', 'matching', "Something went wrong", (array) $e);
            }

            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
