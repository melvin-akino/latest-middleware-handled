<?php

namespace Workers;

use Models\{
    Event,
    EventGroup,    
    MasterEvent,
    UnmatchedEvent
};
use Co\System;
use Exception;

class MatchEvent
{
    public static function handle($dbPool, $event) 
    {
        while (true)
        {
            try {
                $connection = $dbPool->borrow();
            
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

                            $eventGroupResult = EventGroup::checkIfMatched($connection, $event['event_id']);
                            $eventGroupData   = $connection->fetchArray($eventGroupResult);
                            if (!$eventGroupData) {
                                //create a new record in the pivot table event_groups
                                $eventGroup = [
                                    'master_event_id'   => $masterEvent['id'],
                                    'event_id'          => $event['event_id']
                                ];
                                $eventGroupResult = EventGroup::create($connection, $eventGroup);
                            }

                            //Delete it from the unmatched table
                            UnmatchedEvent::deleteUnmatched($connection, $event['event_id']);
                        } else {
                            logger('info', 'matching', "Event remained unmatched", $event);
                            continue;    
                        }
                    }
                }
            } catch (Exception $e) {
                logger('info', 'matching', "Something went wrong", (array) $e);
            } finally {
                $dbPool->return($connection);
                System::sleep(5);
            }
        }
    }
}
