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

            //Fetching of unmatched event_markets records
            $unmatchedMarketsResult = EventMarket::getAllUnmatchedMarket($connection);
            if ($connection->numRows($unmatchedMarketsResult)) {
                $unmatchedMarkets = $connection->fetchAll($unmatchedMarketsResult);
                foreach ($unmatchedMarkets as $unmatchedEventMarket) {
                    $eventId = $unmatchedEventMarket['event_id'];

                    //Fetching the event_group data using the event_id from the unmatched market data
                    $eventGroupResult = EventGroup::getDataByEventId($connection, $eventId);
                    if ($connection->numRows($eventGroupResult)) {
                        $event = $connection->fetchAssoc($eventGroupResult);
                        $masterEventId = $event['master_event_id'];

                        $eventGroupResult = EventGroup::getEventsByMasterEventId($connection, $masterEventId);
                        if ($connection->numRows($eventGroupResult)) {
                            $eventGroups = $connection->fetchAll($eventGroupResult);
                            foreach ($eventGroups as $eventGroup) {
                                if ($eventGroup['event_id'] != $eventId) {
                                    $matchedMarketResult = EventMarket::getMarketsByEventId($connection, $eventGroup['event_id']);
                                    if ($connection->numRows($matchedMarketResult)) {
                                        $matchedEventMarkets = $connection->fetchAll($matchedMarketResult);
                                        foreach ($matchedEventMarkets as $matchedEventMarket) {
                                            if (
                                                $matchedEventMarket['odd_type_id'] == $unmatchedEventMarket['odd_type_id'] &&
                                                $matchedEventMarket['market_flag'] == $unmatchedEventMarket['market_flag'] &&
                                                $matchedEventMarket['odd_label'] == $unmatchedEventMarket['odd_label'] &&
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
                                                continue 3;
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
            }
            
            
            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
