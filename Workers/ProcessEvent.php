<?php

namespace Workers;

use Exception;
use Carbon\Carbon;
use Models\{
    SystemConfiguration,
    Event,
    EventMarket
};

class ProcessEvent
{
    public static function handle($connection, $swooleTable, $message, $offset)
    {
        logger('info', 'event', 'Process Event starting ' . $offset);
        try {
            $eventsTable          = $swooleTable['events'];
            $eventMarketListTable = $swooleTable['eventMarketList'];
            $eventMarketsTable    = $swooleTable['eventMarkets'];
            $sportId              = $message["data"]["sport"];
            $schedule             = $message["data"]["schedule"];

            // First we get the provider_id in the message, and error out if its missing.
            $providerId = $swooleTable['enabledProviders'][$message["data"]["provider"]]["value"];

            if (!$providerId) {
                logger('error', 'event', 'Got event message with no Provider ID on offset ' . $offset);
                return;
            }
            // Next we get the events in the message, and error out if they are missing.
            $payloadEventIDs = $message["data"]["event_ids"];

            // Get the max missingCount from site settings
            // TODO: cache this in Swoole Table
            $missingCountResult = SystemConfiguration::getMaxMissingCount4Deletion($connection, $schedule);
            $missingCount       = $connection->fetchArray($missingCountResult);

            // Now we go through all the message events, and decide which to undelete
            foreach ($payloadEventIDs as $pe) {
                // If an event is in the message that is NOT in the cache, add it to cache
                // and undelete it in DB
                $eventIndexHash = md5(implode(':', [$sportId, $providerId, $pe]));
                if (!$eventsTable->exists($eventIndexHash)) {
                    // Add to cache
                    $eventsTable[$eventIndexHash]['sport_id']         = $sportId;
                    $eventsTable[$eventIndexHash]['event_identifier'] = $pe;
                    $eventsTable[$eventIndexHash]['game_schedule']    = $schedule;
                    $eventsTable[$eventIndexHash]['provider_id']      = $providerId;
                    $eventsTable[$eventIndexHash]['missing_count']    = 0;

                    // Find it in DB
                    $myEventResult = Event::getEventByProviderParam($connection, $pe, $providerId, $sportId);
                    $myEvent       = $connection->fetchArray($myEventResult);

                    // if its not in DB, warn to log file
                    if (!$myEvent) {
                        logger('error', 'event', 'Got event message for event that is NOT in db on offset ' . $offset);
                        break;
                    } else {
                        // First need to find the event in DB because... for some reason...
                        // events have master_event_id in the event table... why????
                        //
                        // update the event to undelete
                        Event::update($connection, [
                            'deleted_at' => null
                        ], [
                            'event_identifier' => $pe,
                            'provider_id'      => $providerId,
                            'sport_id'         => $sportId
                        ]);
                    }
                }
            }

            // Last thing to do is go through the cache and find events in the same
            // sport, provider, schedule that are MISSING from the kafka message...
            // Then we remove from cache and delete in db
            foreach ($eventsTable as $k => $eT) {
                $eventIndexHash = md5(implode(':', [$sportId, $providerId, $k]));

                if ($eT["provider_id"] == $providerId && $eT["sport_id"] == $sportId && $eT["game_schedule"] == $schedule) {
                    if (!in_array($eT['event_identifier'], $payloadEventIDs)) {
                        $eventsTable->incr($eventIndexHash, 'missing_count', 1);
                        if ($eventsTable[$eventIndexHash]["missing_count"] >= $missingCount->value) {
                            $eventsTable->del($eventIndexHash);

                            $myEventResult = Event::getEventByProviderParam($connection, $eT['event_identifier'], $providerId, $sportId);
                            $myEvent       = $connection->fetchArray($myEventResult);

                            if ($myEvent) {

                                $eventId = $myEvent['id'];

                                Event::update($connection, [
                                    'deleted_at' => Carbon::now()
                                ], [
                                    'event_identifier' => $eT['event_identifier'],
                                    'provider_id'      => $providerId,
                                    'sport_id'         => $sportId
                                ]);

                                $activeEventMarkets = explode(',', $eventMarketListTable->get($eventId, 'marketIDs'));
                                foreach ($activeEventMarkets as $marketId) {
                                    if (!empty($marketId)) {
                                        EventMarket::update($connection, [
                                            'deleted_at' => Carbon::now()
                                        ], [
                                            'bet_identifier' => $marketId,
                                            'provider_id'    => $providerId
                                        ]);

                                        $eventMarketsTable->del(implode(':', [$sportId, $providerId, $marketId]));
                                    }
                                }
                                logger('info', 'event', 'Event deleted event identifier ' . $eT['event_identifier']);
                            }
                        } else {
                            Event::update($connection, [
                                'missing_count' => $eventsTable[$k]["missing_count"]
                            ], [
                                'event_identifier' => $eT['event_identifier'],
                                'provider_id'      => $providerId,
                                'sport_id'         => $sportId
                            ]);
                        }
                    }
                }
            }
            logger('info', 'event', 'Process Event ended ' . $offset);
        } catch (Exception $e) {
            logger('error', 'event', 'Exception Error', $e);
        }
    }
}
