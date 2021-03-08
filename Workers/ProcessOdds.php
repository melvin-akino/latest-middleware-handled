<?php

namespace Workers;

use Exception;
use Carbon\Carbon;
use Models\{
    SystemConfiguration,
    League,
    LeagueGroup,
    MasterLeague,
    Team,
    TeamGroup,
    MasterTeam,
    Event,
    EventGroup,
    MasterEvent,
    EventMarket,
    EventMarketGroup,
    MasterEventMarket
};

class ProcessOdds
{
    const SCHEDULE_EARLY  = 'early';
    const SCHEDULE_TODAY  = 'today';
    const SCHEDULE_INPLAY = 'inplay';

    public static function handle($connection, $swooleTable, $message, $offset)
    {
        logger('info', 'odds', 'Process Odds starting ' . $offset);
        try {
            $leaguesTable         = $swooleTable['leagues'];
            $teamsTable           = $swooleTable['teams'];
            $eventsTable          = $swooleTable['events'];
            $eventMarketsTable    = $swooleTable['eventMarkets'];
            $eventMarketListTable = $swooleTable['eventMarketList'];
            $providersTable       = $swooleTable['enabledProviders'];
            $sportsOddTypesTable  = $swooleTable['sportsOddTypes'];

            $messageData       = $message["data"];
            $sportId           = $messageData["sport"];
            $provider          = $messageData["provider"];
            $providerId        = $providersTable[$provider]['value'];
            $homeTeam          = $messageData["homeTeam"];
            $awayTeam          = $messageData["awayTeam"];
            $leagueName        = $messageData["leagueName"];
            $gameSchedule      = $messageData["schedule"];
            $referenceSchedule = date("Y-m-d H:i:s", strtotime($messageData["referenceSchedule"]));
            $homescore         = $messageData["home_score"];
            $awayscore         = $messageData["away_score"];
            $score             = $homescore . ' - ' . $awayscore;
            $runningtime       = $messageData["runningtime"];
            $homeRedcard       = $messageData["home_redcard"];
            $awayRedcard       = $messageData["away_redcard"];
            $events            = $messageData["events"];
            $eventIdentifier   = $events[0]["eventId"];

            $primaryProviderResult = SystemConfiguration::getPrimaryProvider($connection);
            $primaryProvider       = $connection->fetchArray($primaryProviderResult);

            $timestamp = Carbon::now();

            /** leagues */
            $leagueId        = null;
            $leagueIndexHash = md5(implode(':', [$sportId, $providerId, $leagueName]));
            if ($leaguesTable->exists($leagueIndexHash)) {
                $leagueId = $leaguesTable[$leagueIndexHash]['id'];
                logger('info', 'odds', 'league ID from $leaguesTable' . $leagueId);
            } else {
                try {
                    League::create($connection, [
                        'sport_id'    => $sportId,
                        'provider_id' => $providerId,
                        'name'        => $leagueName,
                        'created_at'  => $timestamp
                    ]);
                    $leagueResult = League::lastInsertedData($connection);
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the league');
                    return;
                }

                $league   = $connection->fetchArray($leagueResult);
                $leagueId = $league['id'];

                logger('info', 'odds', 'League Created ' . $leagueId);

                $leaguesTable[$leagueIndexHash] = [
                    'id'          => $leagueId,
                    'sport_id'    => $sportId,
                    'provider_id' => $providerId,
                    'name'        => $leagueName,
                ];
            }

            $masterLeagueId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $leagueGroupResult = LeagueGroup::checkIfMatched($connection, $leagueId);
                $leagueGroupData   = $connection->fetchArray($leagueGroupResult);

                if (!$leagueGroupData) {
                    try {
                        MasterLeague::create($connection, [
                            'sport_id'   => $sportId,
                            'name'       => $leagueName,
                            'created_at' => $timestamp
                        ]);
                        $masterLeagueResult = MasterLeague::lastInsertedData($connection);
                        $masterLeague       = $connection->fetchArray($masterLeagueResult);

                        $masterLeagueId = $masterLeague['id'];
                        logger('info', 'odds', 'Master League Created ' . $masterLeagueId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the master league');
                        return;
                    }

                    try {
                        LeagueGroup::create($connection, [
                            'league_id'        => $leagueId,
                            'master_league_id' => $masterLeagueId
                        ]);
                        logger('info', 'odds', 'League Group Created ' . $leagueId . ' - ' . $masterLeagueId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the league group');
                        return;
                    }
                }
            }


            /** home team **/
            /** teams **/
            $teamHomeId    = null;
            $teamIndexHash = md5($homeTeam . ':' . $sportId . ':' . $providerId);
            if ($teamsTable->exists($teamIndexHash)) {
                $teamHomeId = $teamsTable[$teamIndexHash]['id'];
                logger('info', 'odds', 'team ID from $teamsTable ' . $teamHomeId);
            } else {
                try {
                    Team::create($connection, [
                        'provider_id' => $providerId,
                        'name'        => $homeTeam,
                        'sport_id'    => $sportId,
                        'created_at'  => $timestamp
                    ]);

                    $teamResult = Team::lastInsertedData($connection);
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the team');
                    return;
                }

                $team       = $connection->fetchArray($teamResult);
                $teamHomeId = $team['id'];

                logger('info', 'odds', 'Team Created ' . $teamHomeId);

                $teamsTable[$teamIndexHash] = [
                    'id'          => $teamHomeId,
                    'provider_id' => $providerId,
                    'name'        => $homeTeam,
                    'sport_id'    => $sportId,
                ];
            }
            /** end home team **/

            /** master teams **/
            $masterTeamHomeId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $teamGroupResult = TeamGroup::checkIfMatched($connection, $teamHomeId);
                $teamGroupData   = $connection->fetchArray($teamGroupResult);

                if (!$teamGroupData) {
                    try {
                        MasterTeam::create($connection, [
                            'sport_id'   => $sportId,
                            'name'       => $homeTeam,
                            'created_at' => $timestamp
                        ]);

                        $masterTeamResult = MasterTeam::lastInsertedData($connection);
                        $masterTeam       = $connection->fetchArray($masterTeamResult);

                        $masterTeamHomeId = $masterTeam['id'];

                        logger('info', 'odds', 'Master Team Created ' . $masterTeamHomeId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the master team');
                        return;
                    }

                    try {
                        TeamGroup::create($connection, [
                            'team_id'        => $teamHomeId,
                            'master_team_id' => $masterTeamHomeId
                        ]);

                        $teamGroupsTable[$teamIndexHash] = [
                            'team_id'        => $teamHomeId,
                            'master_team_id' => $masterTeamHomeId,
                        ];

                        logger('info', 'odds', 'Team Groups Created ' . $teamHomeId . ' - ' . $masterTeamHomeId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the team group');
                        return;
                    }
                }
            }

            /** away team **/
            /** teams **/
            $teamAwayId    = null;
            $teamIndexHash = md5($awayTeam . ':' . $sportId . ':' . $providerId);
            if ($teamsTable->exists($teamIndexHash)) {
                $teamAwayId = $teamsTable[$teamIndexHash]['id'];
                logger('info', 'odds', 'team ID from $teamsTable ' . $teamAwayId);
            } else {
                try {
                    Team::create($connection, [
                        'provider_id' => $providerId,
                        'name'        => $awayTeam,
                        'sport_id'    => $sportId,
                        'created_at'  => $timestamp
                    ]);

                    $teamResult = Team::lastInsertedData($connection);
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the team');
                    return;
                }

                $team       = $connection->fetchArray($teamResult);
                $teamAwayId = $team['id'];

                logger('info', 'odds', 'Team Created ' . $teamAwayId);

                $teamsTable[$teamIndexHash] = [
                    'id'          => $teamAwayId,
                    'provider_id' => $providerId,
                    'name'        => $awayTeam,
                    'sport_id'    => $sportId,
                ];
            }
            /** end master away team **/

            /** master away teams **/
            $masterTeamAwayId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $teamGroupResult = TeamGroup::checkIfMatched($connection, $teamAwayId);
                $teamGroupData   = $connection->fetchArray($teamGroupResult);

                if (!$teamGroupData) {
                    try {
                        MasterTeam::create($connection, [
                            'sport_id'   => $sportId,
                            'name'       => $awayTeam,
                            'created_at' => $timestamp
                        ]);

                        $masterTeamResult = MasterTeam::lastInsertedData($connection);
                        $masterTeam       = $connection->fetchArray($masterTeamResult);

                        $masterTeamAwayId = $masterTeam['id'];

                        logger('info', 'odds', 'Master Team Created ' . $masterTeamAwayId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the master team');
                        return;
                    }

                    try {
                        TeamGroup::create($connection, [
                            'team_id'        => $teamAwayId,
                            'master_team_id' => $masterTeamAwayId,
                        ]);

                        $teamGroupsTable[$teamIndexHash] = [
                            'team_id'        => $teamAwayId,
                            'master_team_id' => $masterTeamAwayId,
                        ];

                        logger('info', 'odds', 'Team Groups Created ' . $teamAwayId . ' - ' . $masterTeamAwayId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the team group');
                        return;
                    }
                }
            }
            /** end master away team **/

            /* events */
            $eventId        = null;
            $eventIndexHash = md5(implode(':', [$sportId, $providerId, $eventIdentifier]));
            if ($eventsTable->exists($eventIndexHash)) {
                $eventId = $eventsTable[$eventIndexHash]['id'];

                if ($gameSchedule == self::SCHEDULE_EARLY && $eventsTable[$eventIndexHash]['game_schedule'] == self::SCHEDULE_TODAY) {
                    logger('error', 'odds', 'Event is already in today', $message);
                    return;
                }

                if ($gameSchedule == self::SCHEDULE_TODAY && $eventsTable[$eventIndexHash]['game_schedule'] == self::SCHEDULE_INPLAY) {
                    logger('error', 'odds', 'Event is already in play', $message);
                    return;
                }

                $missingCount = 0;
                Event::update($connection, [
                    'ref_schedule'  => $referenceSchedule,
                    'missing_count' => $missingCount,
                    'game_schedule' => $gameSchedule,
                    'score'         => $score,
                    'running_time'  => $runningtime,
                    'home_penalty'  => $homeRedcard,
                    'away_penalty'  => $awayRedcard,
                    'deleted_at'    => null,
                    'updated_at'    => $timestamp
                ], [
                    'event_identifier' => $eventIdentifier
                ]);

                logger('info', 'odds', 'Event Updated event identifier ' . $eventIdentifier);
            } else {
                $eventResult = Event::getEventByProviderParam($connection, $eventIdentifier, $providerId, $sportId);
                $event       = $connection->fetchArray($eventResult);

                try {
                    $missingCount = 0;
                    if ($event) {
                        Event::update($connection, [
                            'sport_id'      => $sportId,
                            'provider_id'   => $providerId,
                            'league_id'     => $leagueId,
                            'team_home_id'  => $teamHomeId,
                            'team_away_id'  => $teamAwayId,
                            'ref_schedule'  => $referenceSchedule,
                            'missing_count' => $missingCount,
                            'game_schedule' => $gameSchedule,
                            'score'         => $score,
                            'running_time'  => $runningtime,
                            'home_penalty'  => $homeRedcard,
                            'away_penalty'  => $awayRedcard,
                            'updated_at'    => $timestamp,
                            'deleted_at'    => null
                        ], [
                            'event_identifier' => $eventIdentifier
                        ]);

                        logger('info', 'odds', 'Event Updated event identifier ' . $eventIdentifier);
                    } else {
                        Event::create($connection, [
                            'event_identifier' => $eventIdentifier,
                            'sport_id'         => $sportId,
                            'provider_id'      => $providerId,
                            'league_id'        => $leagueId,
                            'team_home_id'     => $teamHomeId,
                            'team_away_id'     => $teamAwayId,
                            'ref_schedule'     => $referenceSchedule,
                            'missing_count'    => $missingCount,
                            'game_schedule'    => $gameSchedule,
                            'score'            => $score,
                            'running_time'     => $runningtime,
                            'home_penalty'     => $homeRedcard,
                            'away_penalty'     => $awayRedcard,
                            'created_at'       => $timestamp
                        ]);

                        $eventResult = Event::lastInsertedData($connection);
                        $event       = $connection->fetchArray($eventResult);

                        logger('info', 'odds', 'Event Created ' . $event['id']);
                    }
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the event');
                    return;
                }

                $eventId = $event['id'];

                $eventsTable[$eventIndexHash] = [
                    'id'               => $eventId,
                    'sport_id'         => $sportId,
                    'provider_id'      => $providerId,
                    'missing_count'    => $missingCount,
                    'league_id'        => $leagueId,
                    'team_home_id'     => $teamHomeId,
                    'team_away_id'     => $teamAwayId,
                    'ref_schedule'     => $referenceSchedule,
                    'game_schedule'    => $gameSchedule,
                    'event_identifier' => $eventIdentifier
                ];
            }
            /* end events */

            $masterEventId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $masterEventUniqueId = date('Ymd', strtotime($referenceSchedule)) . '-' . $sportId . '-' . $masterLeagueId . '-' . $eventIdentifier;

                $eventGroupResult = EventGroup::checkIfMatched($connection, $eventId);
                $eventGroupData   = $connection->fetchArray($eventGroupResult);
                if (!$eventGroupData) {
                    try {
                        MasterEvent::create($connection, [
                            'sport_id'               => $sportId,
                            'master_event_unique_id' => $masterEventUniqueId,
                            'master_league_id'       => $masterLeagueId,
                            'master_team_home_id'    => $masterTeamHomeId,
                            'master_team_away_id'    => $masterTeamAwayId,
                            'created_at'             => $timestamp
                        ]);
                        $masterEventResult = MasterEvent::lastInsertedData($connection);
                        $masterEvent       = $connection->fetchArray($masterEventResult);

                        $masterEventId = $masterEvent['id'];

                        logger('info', 'odds', 'Master Event Created ' . $masterEventId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the master event');
                        return;
                    }

                    try {
                        EventGroup::create($connection, [
                            'event_id'        => $eventId,
                            'master_event_id' => $masterEventId,
                        ]);

                        logger('info', 'odds', 'Event Groups Created ' . $eventId . ' - ' . $masterEventId);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the event group');
                        return;
                    }
                }
            }

            $currentMarketsParsed     = [];
            $activeEventMarkets       = [];
            $activeMasterEventMarkets = [];
            $newMarkets               = [];
            $newMasterMarkets         = [];
            if ($eventMarketListTable->exists($eventId)) {
                $activeEventMarkets = $currentMarkets = explode(',', $eventMarketListTable[$eventId]['marketIDs']);
                if (is_array($currentMarkets)) {
                    foreach ($currentMarkets as $currentMarket) {
                        $currentMarketsParsed[$currentMarket] = 1;
                    }
                }
            }

            /* master event market  and event market*/
            foreach ($events as $event) {
                if (!empty($event)) {
                    $marketEventIdentifiers[] = $event["eventId"];
                    $marketType               = $event["market_type"] == 1 ? 1 : 0;//true or false
                    $marketOdds               = $event["market_odds"];

                    foreach ($marketOdds as $odd) {
                        $selections = $odd["marketSelection"];
                        if (empty($selections)) {
                            EventMarket::softDelete($connection, 'event_id', $eventId);

                            if (is_array($activeEventMarkets)) {
                                foreach ($activeEventMarkets as $marketId) {
                                    $eventMarketsTable->del(implode(':', [$sportId, $providerId, $marketId]));
                                }
                            }
                            $eventMarketListTable->del($eventId);

                            logger('info', 'odds', 'Event Market Deleted with event ID ' . $eventId);
                            break 2;
                        }
                        //TODO: how do we fill this table??
                        if (!$sportsOddTypesTable->exists($sportId . '-' . $odd["oddsType"])) {
                            logger('error', 'odds', 'Odds Type doesn\'t exist', $message);
                            return;
                        }

                        $oddTypeId = $sportsOddTypesTable[$sportId . '-' . $odd["oddsType"]]['value'];
                        foreach ($selections as $selection) {
                            $marketFlag = strtoupper($selection["indicator"]);
                            $marketId  = $selection["market_id"];
                            if ($marketId == "") {
                                if (is_array($activeEventMarkets)) {
                                    foreach ($activeEventMarkets as $activeEventMarket) {
                                        $eventMarket = $eventMarketsTable[implode(':', [$sportId, $providerId, $activeEventMarket])];
                                        if (
                                            $eventMarket['odd_type_id'] == $oddTypeId &&
                                            $eventMarket['provider_id'] == $providerId &&
                                            $eventMarket['market_event_identifier'] == $event["eventId"] &&
                                            $eventMarket['market_flag'] == $marketFlag
                                        ) {
                                            EventMarket::softDelete($connection, 'bet_identifier', $activeEventMarket);
                                            $eventMarketsTable->del(implode(':', [$sportId, $providerId, $activeEventMarket]));

                                            logger('info', 'odds', 'Event Market Deleted with bet identifier ' . $activeEventMarket);
                                            break;
                                        }
                                    }
                                }

                                continue;
                            }

                            $odds   = $selection["odds"];
                            $points = array_key_exists('points', $selection) ? $selection["points"] : "";

                            if (!empty($currentMarketsParsed[$marketId])) {
                                unset($currentMarketsParsed[$marketId]);
                            }

                            if (gettype($odds) == 'string') {
                                $odds = explode(' ', $selection["odds"]);

                                if (count($odds) > 1) {
                                    $points = $points == "" ? $odds[0] : $points;
                                    $odds   = $odds[1];
                                } else {
                                    $odds = $odds[0];
                                }
                            }

                            $odds = trim($odds) == '' ? 0 : (float) $odds;

                            if (!empty($odds)) {
                                $eventMarketId        = null;
                                $eventMarketIndexHash = md5(implode(':', [$providerId, $marketId]));
                                if ($eventMarketsTable->exists($eventMarketIndexHash)) {
                                    $eventMarketId = $eventMarketsTable[$eventMarketIndexHash]['id'];
                                    try {
                                        if (!(
                                            $eventMarket['odds'] == $odds &&
                                            $eventMarket['odd_label'] == $points &&
                                            $eventMarket['is_main'] == $marketType &&
                                            $eventMarket['market_flag'] == $marketFlag &&
                                            $eventMarket['event_id'] == $eventId &&
                                            $eventMarket['market_event_identifier'] == $event["eventId"]
                                        )) {
                                            EventMarket::updateDataByEventMarketId($connection, $eventMarketId, [
                                                'odds'                    => $odds,
                                                'odd_label'               => $points,
                                                'is_main'                 => $marketType,
                                                'market_flag'             => $marketFlag,
                                                'event_id'                => $eventId,
                                                'market_event_identifier' => $event["eventId"],
                                                'deleted_at'              => null,
                                                'updated_at'              => $timestamp
                                            ]);

                                            logger('info', 'odds', 'Event Market Update event market ID ' . $eventMarketId);
                                        }
                                    } catch (Exception $e) {
                                        logger('error', 'odds', 'Another worker already updated the event market');
                                        return;
                                    }
                                } else {
                                    $eventMarketResult = EventMarket::getDataByBetIdentifier($connection, $marketId);
                                    $eventMarket       = $connection->fetchArray($eventMarketResult);
                                    if (!empty($eventMarket['id'])) {
                                        $eventMarketId = $eventMarket['id'];
                                        try {
                                            EventMarket::updateDataByEventMarketId($connection, $eventMarketId, [
                                                'event_id'                => $eventId,
                                                'odd_type_id'             => $oddTypeId,
                                                'odds'                    => $odds,
                                                'odd_label'               => $points,
                                                'bet_identifier'          => $marketId,
                                                'is_main'                 => $marketType,
                                                'market_flag'             => $marketFlag,
                                                'provider_id'             => $providerId,
                                                'market_event_identifier' => $event["eventId"],
                                                'deleted_at'              => null,
                                                'updated_at'              => $timestamp
                                            ]);

                                            logger('info', 'odds', 'Event Market Update event market ID ' . $eventMarketId);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already updated the event market');
                                            return;
                                        }
                                    } else {
                                        try {
                                            EventMarket::create($connection, [
                                                'event_id'                => $eventId,
                                                'odd_type_id'             => $oddTypeId,
                                                'odds'                    => $odds,
                                                'odd_label'               => $points,
                                                'bet_identifier'          => $marketId,
                                                'is_main'                 => $marketType,
                                                'market_flag'             => $marketFlag,
                                                'provider_id'             => $providerId,
                                                'market_event_identifier' => $event["eventId"],
                                                'deleted_at'              => null,
                                                'created_at'              => $timestamp
                                            ]);
                                            $eventMarketResult = EventMarket::lastInsertedData($connection);
                                            $eventMarket       = $connection->fetchArray($eventMarketResult);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already created the event market');
                                            return;
                                        }
                                        $eventMarketId = $eventMarket['id'];

                                        logger('info', 'odds', 'Event Market Created ' . $eventMarketId);
                                    }
                                }

                                $eventMarketsTable[$eventMarketIndexHash] = [
                                    'id'                      => $eventMarketId,
                                    'bet_identifier'          => $marketId,
                                    'event_id'                => $eventId,
                                    'provider_id'             => $providerId,
                                    'odd_type_id'             => $oddTypeId,
                                    'market_event_identifier' => $event["eventId"],
                                    'market_flag'             => $marketFlag,
                                    'is_main'                 => $marketType,
                                    'odd_label'               => $points,
                                    'odds'                    => $odds,
                                ];

                                $newMarkets[] = $marketId;
                                $eventMarketListTable->set($eventId, ['marketIDs' => implode(',', $newMarkets)]);

                                $masterEventMarketId = null;
                                if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                                    $memUID                 = md5($eventId . strtoupper($marketFlag) . $marketId);
                                    $eventMarketGroupResult = EventMarketGroup::checkIfMatched($connection, $eventMarketId);
                                    $eventMarketGroupData   = $connection->fetchArray($eventMarketGroupResult);
                                    if (!$eventMarketGroupData) {
                                        try {
                                            MasterEventMarket::create($connection, [
                                                'master_event_id'               => $masterEventId,
                                                'master_event_market_unique_id' => $memUID,
                                                'created_at'                    => $timestamp
                                            ]);
                                            $masterEventMarketResult = MasterEventMarket::lastInsertedData($connection);
                                            $masterEventMarket       = $connection->fetchArray($masterEventMarketResult);

                                            $masterEventMarketId = $masterEventMarket['id'];

                                            logger('info', 'odds', 'Master Event Market Created ' . $masterEventMarketId);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already created the master event market');
                                            return;
                                        }

                                        try {
                                            EventMarketGroup::create($connection, [
                                                'event_market_id'        => $eventMarketId,
                                                'master_event_market_id' => $masterEventMarketId,
                                            ]);

                                            $eventMarketGroupsTable[$eventMarketIndexHash] = [
                                                'event_market_id'        => $eventMarketId,
                                                'master_event_market_id' => $masterEventMarketId,
                                            ];


                                            logger('info', 'odds', 'Event Market Groups Created ' . $eventMarketId . ' - ' . $masterEventMarketId);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already created the event market group');
                                            return;
                                        }
                                    }
                                }
                            } else {
                                if (is_array($activeEventMarkets) && in_array($marketId, $activeEventMarkets)) {
                                    EventMarket::softDelete($connection, 'bet_identifier', $marketId);

                                    $eventMarketsTable->del($marketId);

                                    logger('info', 'odds', 'Event Market Deleted with bet identifier ' . $marketId);
                                }
                            }
                        }
                    }
                }
            }
            /* end  master event market  and event market*/

            $noLongerActiveMarkets = $currentMarketsParsed;
            if (!empty($noLongerActiveMarkets) && is_array($activeEventMarkets)) {
                foreach ($activeEventMarkets as $activeEventMarket) {
                    if (
                    array_key_exists($activeEventMarket, $noLongerActiveMarkets)
                    ) {
                        EventMarket::softDelete($connection, 'bet_identifier', $activeEventMarket);
                        $eventMarketsTable->del($activeEventMarket);

                        logger('info', 'odds', 'Event Market Deleted with bet identifier ' . $activeEventMarket);
                    }
                }
            }
        } catch (Exception $e) {
            logger('error', 'odds', $e, $message);
        }
    }
}
