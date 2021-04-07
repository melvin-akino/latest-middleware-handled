<?php

namespace Workers;

use Models\{
    EventMarket,
    EventMarketGroup,
    MasterEventMarket,
    Event,
    EventGroup,
    Provider
};
use Co\System;
use Exception;
use Carbon\Carbon;

class MatchEventMarket
{
    public static function handle($dbPool, $swooleTable)
    {
        while (true) {

            try {
                $connection = $dbPool->borrow();

                $primaryProviderId    = Provider::getIdByAlias($connection, $swooleTable['systemConfig']['PRIMARY_PROVIDER']['value']);

                $primaryEventMarkets = [];
                $rawActiveEventMarkets = $swooleTable['eventMarkets'];
                if ($rawActiveEventMarkets->count() > 0) {
                    foreach ($rawActiveEventMarkets as $eventMarket) {
                        if ($eventMarket['provider_id'] == $primaryProviderId) {
                            $primaryEventMarkets[$eventMarket['id']] = $eventMarket['id'];
                        }
                    }

                    if (!empty($primaryEventMarkets)) {
                        $unmatchedMarketResult = EventMarket::getUnmatchedMarketByIds($connection, $primaryEventMarkets);
                        if ($connection->numRows($unmatchedMarketResult)) {
                            $unmatchedMarkets = $connection->fetchAll($unmatchedMarketResult);
                            if (is_array($unmatchedMarkets)) {
                                foreach ($unmatchedMarkets as $unmatchMarket) {
                                    $memUID = md5($unmatchMarket['event_id'] . strtoupper($unmatchMarket['market_flag']) . $unmatchMarket['bet_identifier']);
                                    $matchedEvent = $swooleTable['matchedEvents'][$unmatchMarket['event_id']];
                                    if ($matchedEvent && !empty($matchedEvent['master_event_id'])) {
                                        $masterEventMarketResult = MasterEventMarket::create($connection, [
                                            'master_event_id'               => $matchedEvent['master_event_id'],
                                            'master_event_market_unique_id' => $memUID,
                                            'created_at'                    => Carbon::now()
                                        ], 'id');

                                        $masterEventMarket = $connection->fetchArray($masterEventMarketResult);
                                        $toMatchData = [
                                            'master_event_market_id' => $masterEventMarket['id'],
                                            'event_market_id'        => $unmatchMarket['id']
                                        ];
                                        $eventMarketGroupResult = EventMarketGroup::create($connection, $toMatchData);
                                        if ($eventMarketGroupResult) {
                                            logger('info', 'matching', 'Market matched', $toMatchData);
                                        }
                                    }
                                }
                            } else {
                                logger('info', 'matching', 'Nothing to match');
                            }
                        } else {
                            logger('info', 'matching', 'Nothing to match');
                        }
                    } else {
                        logger('info', 'matching', 'Nothing to match');
                    }
                }

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
