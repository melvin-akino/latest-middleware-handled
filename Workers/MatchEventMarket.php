<?php

namespace Workers;

use Models\{
    EventMarket,
    EventMarketGroup,
    Event,
    EventGroup
};
use Co\System;
use Exception;

class MatchEventMarket
{
    public static function handle($dbPool, $swooleTable)
    {
        while (true) {
            $connection = $dbPool->borrow();
var_dump("======================");
            $umatchedMarketsResult = EventMarket::getAllUnmatchedMarket($connection);//event ID 4, 5, 6
            while ($unmatchedEventMarket = $connection->fetchAssoc($umatchedMarketsResult)) {
                $eventId = $unmatchedEventMarket['event_id'];
                $eventGroupResult = EventGroup::getDataByEventId($connection, $eventId);//event ID 4
                // var_dump($eventGroupResult);
                var_dump($eventId);
                if ($connection->numRows($eventGroupResult) ) {echo 1;
                    $event = $connection->fetchAssoc($eventGroupResult);
                    $masterEventId = $event['master_event_id'];

                    $eventGroupResult = EventGroup::getEventsByMasterEventId($connection, $masterEventId);
                    while ($eventGroup = $connection->fetchAssoc($eventGroupResult)) {
                        if ($eventGroup['event_id'] != $eventId) {
                            $matchedMarketResult = EventMarket::getMarketsByEventId($connectipn, $eventGroup['event_id']);
                            if ($connection->numRows($matchedMarketResult)) {
                                while ($matchedEventMarket = $connection->fetchAssoc($matchedMarketResult)) {
                                    if (
                                        $matchedEventMarket['odd_type_id'] == $unmatchedEventMarket['odd_type_id'] &&
                                        $matchedEventMarket['market_flag'] == $unmatchedEventMarket['market_flag'] &&
                                        $matchedEventMarket['points'] == $unmatchedEventMarket['points'] &&
                                        !empty($matchedEventMarket['master_event_market_id'])
                                    ) {
                                        $toMatchData = [
                                            'master_event_market_id' => $matchedEventMarket['master_event_market_id'],
                                            'event_market_id' => $unmatchedEventMarket['id']
                                        ];
                                        $eventMarketGroupResult = EventMarketGroup::create($connection, $toMatchData);

                                        if ($eventMarketGroupResult) {
                                            logger('info', 'matching', "Market matched", $toMatchData);
                                        } else {
                                            logger('error', 'matching', "Something went wrong.", $toMatchData);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    logger('info', 'matching', "Market remained unmatched", $unmatchedEventMarket);
                    continue;
                }
            }
            
            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
