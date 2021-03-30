<?php

namespace Workers;

use Exception;
use Carbon\Carbon;
use Models\{
    MasterLeague,
    SystemConfiguration,
    Event,
    EventGroup,
    EventMarket,
    EventMarketGroup,
    MasterEventMarket,
    UnmatchedData
};
use Ramsey\Uuid\Uuid;

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

                        $eventsTable[$eventIndexHash]['id'] = $myEvent['id'];
                    }
                }
            }

            // Last thing to do is go through the cache and find events in the same
            // sport, provider, schedule that are MISSING from the kafka message...
            // Then we remove from cache and delete in db
            foreach ($eventsTable as $k => $eT) {
                $eventIndexHash = md5(implode(':', [$sportId, $providerId, $k]));

                if ($eT["provider_id"] == $providerId && $eT["sport_id"] == $sportId && $eT["game_schedule"] == $schedule) {
                    if (!in_array($eT['event_identifier'], $payloadEventIDs) && !empty($eT['event_identifier'])) {
                        $eventsTable->incr($eventIndexHash, 'missing_count', 1);
                        if ($eventsTable[$eventIndexHash]["missing_count"] >= $missingCount->value) {
                            $eventsTable->del($eventIndexHash);

                            $myEventResult = Event::getEventByProviderParam($connection, $eT['event_identifier'], $providerId, $sportId);
                            $myEvent       = $connection->fetchArray($myEventResult);

                            if ($myEvent) {

                                $eventId = $myEvent['id'];

                                $myMasterEventResult = EventGroup::getDataByEventId($connection, $eventId);
                                $myMasterEvent = $connection->fetchAssoc($myMasterEventResult);

                                $myMatchedEventsResult = EventGroup::getMatchedEvents($connection, $myMasterEvent['master_event_id'], $eventId);
                                $myMatchedEvents = $connection->fetchAll($myMatchedEventsResult);

                                foreach ($myMatchedEvents as $matchedEvent) {
                                    UnmatchedData::create($connection, [
                                        'provider_id' => $providerId,
                                        'data_type' => 'event',
                                        'data_id' => $matchedEvent['event_id']
                                    ]);
                                }

                                EventGroup::deleteMatchesOfEvent($connection, $myMasterEvent['master_event_id'], $eventId);

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

                                        $myEventMarketResult = EventMarket::getDataByBetIdentifier($connection, $marketId);
                                        $myEventMarket = $connection->fetchAssoc($myEventMarketResult);

                                        $myEventMarketGroupResult = EventMarketGroup::getDataByEventMarketId($connection, $myEventMarket['id']);
                                        $myEventMarketGroup = $connection->fetchAssoc($myEventMarketGroupResult);

                                        EventMarketGroup::deleteMatchesOfEventMarket($connection, $myEventMarketGroup['master_event_market_id']);

                                        EventMarket::update($connection, [
                                            'deleted_at' => Carbon::now()
                                        ], [
                                            'bet_identifier' => $marketId,
                                            'provider_id'    => $providerId
                                        ]);

                                        $eventMarketsTable->del(md5(implode(':', [$providerId, $marketId])));
                                    }
                                }
                                
                                MasterEventMarket::deleteMasterEventMarketByMasterEventId($connection, $myMasterEvent['master_event_id']);
                                
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

            if ($message["data"]["provider"] == $swooleTable['systemConfig']['PRIMARY_PROVIDER']['value']) {
                $sidebarLeagues = MasterLeague::getSideBarLeaguesBySportAndGameSchedule(
                    $connection,
                    $sportId,
                    $swooleTable['enabledProviders'][$swooleTable['systemConfig']['PRIMARY_PROVIDER']['value']]["value"],
                    $swooleTable['systemConfig']['EVENT_VALID_MAX_MISSING_COUNT']['value'],
                    $schedule
                );
                $sidebarResult = $connection->fetchAll($sidebarLeagues);

                self::sendToKafka($sidebarResult, $schedule);
            }

            logger('info', 'event', 'Process Event ended ' . $offset);
        } catch (Exception $e) {
            logger('error', 'event', 'Exception Error', $e);
        }
    }

    private function sendToKafka($message, $gameSchedule)
    {
        $data[$gameSchedule] = $message ? $message : [];
        $payload             = [
            'request_uid' => (string) Uuid::uuid4(),
            'request_ts'  => getMilliseconds(),
            'command'     => 'sidebar',
            'sub_command' => 'transform',
            'data'        => $data,
        ];

        kafkaPush(getenv('KAFKA_SIDEBAR_LEAGUES'), $payload, $payload['request_uid']);
        logger('info', 'event', '[SIDEBAR-LEAGUES] Payload sent: ' . $payload['request_uid']);
    }
}
