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

            try {
                $connection = $dbPool->borrow();

                $unmatchedMarketsResult = EventMarket::getAllUnmatchedMarketWithMatchedEvents($connection);
                if ($connection->numRows($unmatchedMarketsResult)) {
                    $unmatchedMarkets = $connection->fetchAll($unmatchedMarketsResult);
                    foreach ($unmatchedMarkets as $unmatchedEventMarket) {
                        $eventId = $unmatchedEventMarket['event_id'];

                        $masterEventId = $unmatchedEventMarket['master_event_id'];
                        $matchedMarketResult = EventMarket::getMarketsByMasterEventId($connection, $masterEventId);
                        if ($connection->numRows($matchedMarketResult)) {
                            $matchedEventMarkets = $connection->fetchAll($matchedMarketResult);
                            foreach ($matchedEventMarkets as $matchedEventMarket) {
                                if (
                                    $matchedEventMarket['odd_type_id'] == $unmatchedEventMarket['odd_type_id'] &&
                                    $matchedEventMarket['market_flag'] == $unmatchedEventMarket['market_flag'] &&
                                    $matchedEventMarket['odd_label'] == $unmatchedEventMarket['odd_label'] &&
                                    $matchedEventMarket['event_id'] != $eventId &&
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
                
                
                $dbPool->return($connection);
            } catch (Exception $e) {
                logger('error', 'Something went wrong', (array) $e);
            }
            
            System::sleep(10);
        }
    }
}
