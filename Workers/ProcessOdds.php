<?php

namespace Workers;

use Models\{
    ProviderAccount,
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
    const SCHEDULE_EARLY = 'early';
    const SCHEDULE_TODAY = 'today';
    const SCHEDULE_INPLAY = 'inplay';

    public static function handle($connection, $swooleTable, $message, $offset)
    {
        logger('info', 'odds', 'Process Odds starting ' . $offset);
        try {
            // $masterLeaguesIndexTable = $swooleTable['masterLeaguesIndex'];
            // $leaguesIndexTable       = $swooleTable['leaguesIndex'];

            $leagueGroupsTable       = $swooleTable['leagueGroups'];
            $leaguesTable            = $swooleTable['leagues'];
            $masterLeaguesTable      = $swooleTable['masterLeagues'];

            // $masterTeamsIndexTable = $swooleTable['masterTeamsIndex'];
            // $teamsIndexTable       = $swooleTable['teamsIndex'];

            $teamGroupsTable       = $swooleTable['teamGroups'];
            $teamsTable            = $swooleTable['teams'];
            $masterTeamsTable      = $swooleTable['masterTeams'];

            $eventsTable                = $swooleTable['events'];
            $eventGroupsTable           = $swooleTable['eventGroups'];
            $masterEventsTable          = $swooleTable['masterEvents'];

            // $masterEventsIndexTable     = $swooleTable['masterEventsIndex'];
            // $masterEventsIndexKeysTable = $swooleTable['masterEventsIndexKeys'];
            
            // $eventsIndexTable           = $swooleTable['eventsIndex'];
            // $eventsIndexKeysTable       = $swooleTable['eventsIndexKeys'];

            $eventMarketGroupsTable       = $swooleTable['eventMarketGroups'];
            $eventMarketsTable            = $swooleTable['eventMarkets'];
            $masterEventMarketsTable      = $swooleTable['masterEventMarkets'];
            $masterEventMarketsIndexTable = $swooleTable['masterEventMarketsIndex'];

            
            $eventMarketsIndexTable       = $swooleTable['eventMarketsIndex'];

            $eventMarketListTable       = $swooleTable['eventMarketList'];
            $masterEventMarketListTable = $swooleTable['masterEventMarketList'];

            $providersTable             = $swooleTable['enabledProviders'];

            $sportsOddTypesTable = $swooleTable['sportsOddTypes'];

            $messageData       = $message["data"];
            $requestTs         = $message["request_ts"];
            $requestUid        = $message["request_uid"];
            $sportId           = $messageData["sport"];
            $provider          = $messageData["provider"];
            $providerId        = $providersTable[$provider]['value'];
            $homeTeam          = $messageData["homeTeam"];
            $awayTeam          = $messageData["awayTeam"];
            $leagueName        = $messageData["leagueName"];
            $schedule          = $messageData["schedule"];
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


            /** leagues */
            $leagueId        = null;
            $leagueIndexHash = md5(implode(':', [$sportId, $providerId, $leagueName]));
            if ($leaguesTable->exists($leagueIndexHash)) {
                $leagueId = $leaguesTable[$leagueIndexHash]['league_id'];
                logger('info', 'odds', 'league ID from $leaguesTable' . $leagueId);
            } else {
                try {
                    League::create($connection, [
                        'sport_id'         => $sportId,
                        'provider_id'      => $providerId,
                        'name'             => $leagueName
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
                    'id'               => $leagueId,
                    'sport_id'         => $sportId,
                    'provider_id'      => $providerId,
                    'name'             => $leagueName,
                ];
            }
            
            $masterLeagueId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                if (!$leagueGroupsTable->exists($leagueIndexHash)) {
                    $masterLeagueIndexHash = md5($sportId . ':' . $leagueName);
                    if (!$masterLeaguesTable->exists($masterLeagueIndexHash)) {
                        try {
                            MasterLeague::create($connection, [
                                'sport_id' => $sportId,
                                'name'     => $leagueName,
                            ]);
                            $masterLeagueResult = MasterLeague::lastInsertedData($connection);
                            $masterLeague = $connection->fetchArray($masterLeagueResult);

                            $masterLeagueId = $masterLeague['id'];
    
                            $masterLeaguesTable[$masterLeagueIndexHash] = [
                                'id'       => $masterLeagueId,
                                'sport_id' => $sportId,
                                'name'     => $leagueName,
                            ];

                            logger('info', 'odds', 'Master League Created ' . $masterLeagueId);
                        } catch (Exception $e) {
                            logger('error', 'odds', 'Another worker already created the master league');
                            return;
                        }
                    } else {
                        $masterLeagueId = $masterLeaguesTable[$masterLeagueIndexHash]['id'];
                    }

                    try {
                        LeagueGroup::create($connection, [
                            'league_id'        => $leagueId,
                            'master_league_id' => $masterLeagueId,
                        ]);

                        $leagueGroupsTable[$leagueIndexHash] = [
                            'league_id'        => $leagueId,
                            'master_league_id' => $masterLeagueId,
                        ];
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
                $teamHomeId = $teamsTable[$teamIndexHash]['team_id'];
                logger('info', 'odds', 'team ID from $teamsTable' . $teamHomeId);
            } else {
                try {
                    Team::create($connection, [
                        'provider_id'    => $providerId,
                        'name'           => $homeTeam,
                        'sport_id'       => $sportId,
                    ]);
                    
                    $teamResult = Team::lastInsertedData($connection);
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the team');
                    return;
                }

                $team = $connection->fetchArray($teamResult);
                $teamHomeId = $team['id'];
                
                logger('info', 'odds', 'Team Created ' . $leagueId);

                $teamsTable[$teamIndexHash] = [
                    'id'             => $teamHomeId,
                    'provider_id'    => $providerId,
                    'name'           => $homeTeam,
                    'sport_id'       => $sportId,
                ];
            }
            /** end home team **/

            /** master teams **/
            $masterTeamHomeId    = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                if (!$teamGroupsTable->exists($teamIndexHash)) {
                    $masterTeamIndexHash = md5($homeTeam . ':' . $sportId);
                    if (!$masterTeamsTable->exists($masterTeamIndexHash)) {
                        try {
                            MasterTeam::create($connection, [
                                'sport_id' => $sportId,
                                'name'     => $homeTeam
                            ]);

                            $masterTeamResult = MasterTeam::lastInsertedData($connection);
                            $masterTeam = $connection->fetchArray($masterTeamResult);

                            $masterTeamHomeId = $masterTeam['id'];
    
                            $masterTeamsTable[$masterTeamIndexHash] = [
                                'id' => $masterTeamHomeId,
                                'sport_id' => $sportId,
                                'name'     => $homeTeam
                            ];

                            logger('info', 'odds', 'Master Team Created ' . $masterTeamHomeId);
                        } catch (Exception $e) {
                            logger('error', 'odds', 'Another worker already created the master team');
                            return;
                        }
                    } else {
                        $masterTeamHomeId = $masterTeamsTable[$masterTeamIndexHash]['id'];
                    }

                    try {
                        TeamGroup::create($connection, [
                            'team_id'        => $teamHomeId,
                            'master_team_id' => $masterTeamHomeId,
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
                $teamAwayId = $teamsTable[$teamIndexHash]['team_id'];
                logger('info', 'odds', 'team ID from $teamsTable' . $teamAwayId);
            } else {
                try {
                    Team::create($connection, [
                        'provider_id'    => $providerId,
                        'name'           => $awayTeam,
                        'sport_id'       => $sportId
                    ]);

                    $teamResult = Team::lastInsertedData($connection);
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the team');
                    return;
                }

                $team = $connection->fetchArray($teamResult);
                $teamAwayId = $team['id'];

                logger('info', 'odds', 'Team Created ' . $teamAwayId);

                $teamsTable[$teamIndexHash] = [
                    'id'             => $teamAwayId,
                    'provider_id'    => $providerId,
                    'name'           => $awayTeam,
                    'sport_id'       => $sportId,
                ];
            }
            /** end master away team **/

            /** master away teams **/
            $masterTeamAwayId    = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                if (!$teamGroupsTable->exists($teamIndexHash)) {
                    $masterTeamIndexHash = md5($awayTeam . ':' . $sportId);
                    if (!$masterTeamsTable->exists($masterTeamIndexHash)) {
                        try {
                            MasterTeam::create($connection, [
                                'sport_id' => $sportId,
                                'name'     => $awayTeam
                            ]);

                            $masterTeamResult = MasterTeam::lastInsertedData($connection);
                            $masterTeam = $connection->fetchArray($masterTeamResult);

                            $masterTeamAwayId = $masterTeam['id'];
    
                            $masterTeamsTable[$masterTeamIndexHash] = [
                                'master_team_id' => $masterTeamAwayId,
                                'sport_id' => $sportId,
                                'name'     => $awayTeam
                            ];

                            logger('info', 'odds', 'Master Team Created ' . $masterTeamAwayId);
                        } catch (Exception $e) {
                            logger('error', 'odds', 'Another worker already created the master team');
                            return;
                        }
                    } else {
                        $masterTeamAwayId = $masterTeamsTable[$masterTeamIndexHash]['master_team_id'];
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
            $eventId   = null;
            $eventData = [
                'league_id'       => $leagueId,
                'team_home_id'    => $teamHomeId,
                'team_away_id'    => $teamAwayId,
                'ref_schedule'    => $referenceSchedule
            ];

            $eventIndexHash = md5(implode(':', [$sportId, $providerId, $eventIdentifier]));
            if ($eventsTable->exists($eventIndexHash)) {
                $eventId   = $event['id'];

                if ($schedule == self::SCHEDULE_EARLY && $eventsTable[$eventIndexHash]['game_schedule'] == self::SCHEDULE_TODAY) {
                    logger('error', 'odds', 'Event is already in today', $message);
                    return;
                }

                if ($schedule == self::SCHEDULE_TODAY && $eventsTable[$eventIndexHash]['game_schedule'] == self::SCHEDULE_INPLAY) {
                    logger('error', 'odds', 'Event is already in play', $message);
                    return;
                }

            } else {
                try {
                    $missingCount = 0;
                    Event::create($connection, [
                        'event_identifier' => $eventIdentifier,
                        'sport_id'        => $sportId,
                        'provider_id'     => $providerId,
                        'provider_id'     => $providerId,
                        'league_id'       => $leagueId,
                        'team_home_id'    => $teamHomeId,
                        'team_away_id'    => $teamAwayId,
                        'ref_schedule'    => $referenceSchedule,
                        'missing_count'   => $missingCount,
                        'game_schedule'   => $schedule,
                        'score'           => $score,
                        'running_time'    => $runningtime,
                        'home_penalty'    => $homeRedcard,
                        'away_penalty'    => $awayRedcard
                    ]);
                    $eventResult = Event::lastInsertedData($connection);

                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the event');
                    return;
                }

                $event   = $connection->fetchArray($eventResult);
                $eventId = $event['id'];

                logger('info', 'odds', 'Event Created ' . $eventId);

                $eventsTable[$eventIndexHash] = [
                    'id'               => $eventId,
                    'sport_id'         => $sportId,
                    'provider_id'      => $providerId,
                    'missingCount'    => $missingCount,
                    'league_id'       => $leagueId,
                    'team_home_id'    => $teamHomeId,
                    'team_away_id'    => $teamAwayId,
                    'ref_schedule'    => $referenceSchedule,
                    'game_schedule'   => $schedule,
                ];
            }
            /* end events */

            $masterEventId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $masterEventUniqueId = date('Ymd', strtotime($referenceSchedule)) . '-' . $sportId . '-' . $masterLeagueId . '-' . $eventIdentifier;
                if (!$eventGroupsTable->exists($eventIndexHash)) {
                    $masterEventIndexHash = md5(implode(':', [$sportId, $eventIdentifier]));
                    if (!$masterEventsTable->exists($masterEventIndexHash)) {
                        try {
                            MasterEvent::create($connection, [
                                'sport_id'               => $sportId,
                                'master_event_unique_id' => $masterEventUniqueId,
                                'master_league_id'       => $masterLeagueId,
                                'master_team_home_id'    => $masterTeamHomeId,
                                'master_team_away_id'    => $masterTeamAwayId
                            ]);
                            $masterEventResult = MasterEvent::lastInsertedData($connection);
                            $masterEvent = $connection->fetchArray($masterEventResult);

                            $masterEventId = $masterEvent['id'];
    
                            $masterEventsTable[$masterEventIndexHash] = [
                                'id'       => $masterLeagueId,
                                'sport_id'               => $sportId,
                                'master_event_unique_id' => $masterEventUniqueId,
                                'master_league_id'       => $masterLeagueId,
                                'master_team_home_id'    => $masterTeamHomeId,
                                'master_team_away_id'    => $masterTeamAwayId
                            ];

                            logger('info', 'odds', 'Master Event Created ' . $masterEventId);
                        } catch (Exception $e) {
                            logger('error', 'odds', 'Another worker already created the master event');
                            return;
                        }
                    } else {
                        $masterEventId = $masterEventsTable[$masterEventIndexHash]['id'];
                    }

                    try {
                        EventGroup::create($connection, [
                            'event_id'        => $eventId,
                            'master_event_id' => $masterEventId,
                        ]);

                        $eventGroupsTable[$masterEventIndexHash] = [
                            'event_id'        => $eventId,
                            'master_event_id' => $masterEventId,
                        ];

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

            if ($masterEventMarketListTable->exists($masterEventId)) {
                $activeMasterEventMarkets = explode(',', $masterEventMarketListTable[$masterEventId]['memUIDs']);
            }

            /* master event market  and event market*/
            foreach ($events as $event) {
                if (!empty($event)) {
                    $marketEventIdentifiers[] = $event["eventId"];
                    $marketType               = $event["market_type"] == 1;//true or false
                    $marketOdds               = $event["market_odds"];

                    foreach ($marketOdds as $odd) {
                        $selections = $odd["marketSelection"];
                        if (empty($selections)) {
                            EventMarket::where('event_id', $eventId)->delete();

                            if (is_array($activeEventMarkets)) {
                                foreach ($activeEventMarkets as $marketId) {
                                    // $masterEventMarketId = $eventMarketsTable->get($marketId, 'master_event_market_id');
                                    $eventMarketsTable->del(implode(':', [$sportId, $providerId, $marketId]));
                                    // $eventMarketsIndexTable->del(implode(':', [$eventId, $masterEventMarketId]));
                                    // $masterEventMarketsTable->del($masterEventMarketId);
                                }
                            }
                            // if (is_array($activeMasterEventMarkets)) {
                            //     foreach ($activeMasterEventMarkets as $memUID) {
                            //         $masterEventMarketsIndexTable->del($memUID);
                            //     }
                            // }
                            $masterEventMarketListTable->del($masterEventId);
                            $eventMarketListTable->del($eventId);
                            break 2;
                        }
                        //TODO: how do we fill this table??
                        if (!$sportsOddTypesTable->exists($sportId . '-' . $odd["oddsType"])) {
                            logger('error', 'odds', 'Odds Type doesn\'t exist', $message);
                            return;
                        }

                        $oddTypeId = $sportsOddTypesTable[$sportId . '-' . $odd["oddsType"]]['value'];
                        foreach ($selections as $selection) {
                            $indicator = strtoupper($selection["indicator"]);
                            $marketId  = $selection["market_id"];
                            if ($marketId == "") {
                                if (is_array($activeEventMarkets)) {
                                    foreach ($activeEventMarkets as $activeEventMarket) {
                                        $eventMarket = $eventMarketsTable[$activeEventMarket];
                                        if (
                                            $eventMarket['odd_type_id'] == $oddTypeId &&
                                            $eventMarket['provider_id'] == $providerId &&
                                            $eventMarket['market_event_identifier'] == $event["eventId"] &&
                                            $eventMarket['market_flag'] == $indicator
                                        ) {
                                            // EventMarket::where('bet_identifier', $eventMarket['bet_identifier'])->delete();
                                            EventMarket::softDelete($connection, 'bet_identifier', $eventMarket['bet_identifier']);

                                            // $masterEventMarketId = $eventMarketsTable->get($eventMarket['bet_identifier'], 'master_event_market_id');
                                            $eventMarketsTable->del($eventMarket['bet_identifier']);
                                            // $eventMarketsIndexTable->del(implode(':', [$eventId, $masterEventMarketId]));

                                            // $memUID = $masterEventMarketsTable->get($masterEventMarketId, 'master_event_market_unique_id');
                                            // $masterEventMarketsTable->del($masterEventMarketId);
                                            // $masterEventMarketsIndexTable->del($memUID);

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

                                $logMarket = null;
                                $eventMarketIndexHash = md5(implode(':', [$providerId, $marketId]));
                                if ($eventMarketsTable->exists($eventMarketIndexHash)) {
                                    $eventMarketId = $eventMarketsTable[$eventMarketIndexHash]['id'];
                                    try {
                                        if (!(
                                            $eventMarket['odds'] == $odds &&
                                            $eventMarket['odd_label'] == $points &&
                                            $eventMarket['is_main'] == $marketType &&
                                            $eventMarket['market_flag'] == $indicator &&
                                            $eventMarket['event_id'] == $eventId &&
                                            $eventMarket['market_event_identifier'] == $event["eventId"]
                                        )) {
                                            EventMarket::updateDataByEventMarketId($connection, $eventMarketId, [
                                                'odds'                    => $odds,
                                                'odd_label'               => $points,
                                                'is_main'                 => $marketType,
                                                'market_flag'             => $indicator,
                                                'event_id'                => $eventId,
                                                'market_event_identifier' => $event["eventId"],
                                                'deleted_at'              => null
                                            ]);
                                            $logMarket = 'update';
                                        }
                                    } catch (Exception $e) {
                                        logger('error', 'odds', 'Another worker already updated the event market');
                                        return;
                                    }
                                } else {
                                    $logMarket = 'new';
                                    $eventMarketResult = EventMarket::getDataByBetIdentifier($connection, $marketId);
                                    $eventMarket = $connection->fetchArray($eventMarketResult);
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
                                                'market_flag'             => $indicator,
                                                'provider_id'             => $providerId,
                                                'market_event_identifier' => $event["eventId"],
                                                'deleted_at'              => null
                                            ]);
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
                                                'market_flag'             => $indicator,
                                                'provider_id'             => $providerId,
                                                'market_event_identifier' => $event["eventId"],
                                                'deleted_at'              => null
                                            ]);
                                            $eventMarketResult = EventMarket::lastInsertedData($connection);
                                            $eventMarket   = $connection->fetchArray($eventMarketResult);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already created the event market');
                                            return;
                                        }
                                        $eventMarketId = $eventMarket['id'];
                                    }
                                } 
                                
                                $eventMarketsTable[$eventMarketIndexHash] = [
                                    'id'                      => $eventMarketId,
                                    'bet_identifier'          => $marketId,
                                    'event_id'                => $eventId,
                                    'provider_id'             => $providerId,
                                    'odd_type_id'             => $oddTypeId,
                                    'market_event_identifier' => $event["eventId"],
                                    'market_flag'             => $indicator,
                                    'is_main'                 => $marketType,
                                    'odd_label'               => $points,
                                    'odds'                    => $odds,
                                ];

                                $newMarkets[] = $marketId;
                                $eventMarketListTable->set($eventId, ['marketIDs' => implode(',', $newMarkets)]);

                                $masterEventMarketId    = null;
                                if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                                    $memUID                 = md5($eventId . strtoupper($indicator) . $marketId);
                                    if (!$eventMarketGroupsTable->exists($eventMarketIndexHash)) {
                                        $masterEventMarketIndexHash = $memUID;
                                        if (!$masterEventMarketsTable->exists($masterEventMarketIndexHash)) {
                                            try {
                                                MasterEventMarket::create($connection, [
                                                    'master_event_id'               => $masterEventId,
                                                    'master_event_market_unique_id' => $memUID
                                                ]);
                                                $masterEventMarketResult = MasterEventMarket::lastInsertedData($connection);
                                                $masterEventMarket = $connection->fetchArray($masterEventMarketResult);

                                                $masterEventMarketId = $masterEventMarket['id'];
                        
                                                $masterEventMarketsTable[$masterEventMarketIndexHash] = [
                                                    'id'       => $masterEventMarketId,
                                                    'master_event_id'               => $masterEventId,
                                                    'master_event_market_unique_id' => $memUID
                                                ];
                                            } catch (Exception $e) {
                                                logger('error', 'odds', 'Another worker already created the master event market');
                                                return;
                                            }
                                        } else {
                                            $masterEventMarketId = $masterEventMarketsTable[$masterEventMarketIndexHash]['id'];
                                        }

                                        try {
                                            EventMarketGroup::create($connection, [
                                                'event_market_id'        => $eventMarketId,
                                                'master_event_market_id' => $masterEventMarketId,
                                            ]);

                                            $eventMarketGroupsTable[$leagueIndexHash] = [
                                                'event_market_id'        => $eventMarketId,
                                                'master_event_market_id' => $masterEventMarketId,
                                            ];
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already created the event market group');
                                            return;
                                        }
                                    }

                                    $newMasterMarkets[] = $memUID;
                                    $masterEventMarketListTable->set($masterEventId, ['memUIDs' => implode(',', $newMasterMarkets)]);
                                }



                                

                                // $logsRecord = [
                                //     'master_event_market_id' => $masterEventMarketId,
                                //     'odd_type_id'            => $oddTypeId,
                                //     'odds'                   => $odds,
                                //     'odd_label'              => $points,
                                //     'is_main'                => $marketType,
                                //     'market_flag'            => $indicator,
                                //     'provider_id'            => $providerId
                                // ];

                                // // logs insert only if new records or   update happened
                                // if (!empty($logMarket) && !empty($masterEventMarketId)) {
                                //     $MasterEventMarketLogId = MasterEventMarketLog::create($logsRecord);
                                // }
                            } else {
                                if (is_array($activeEventMarkets) && in_array($marketId, $activeEventMarkets)) {
                                    EventMarket::softDelete($connection, 'bet_identifier', $marketId);

                                    // $masterEventMarketId = $eventMarketsTable->get($marketId, 'master_event_market_id');
                                    $eventMarketsTable->del($marketId);
                                    // $eventMarketsIndexTable->del(implode(':', [$eventId, $masterEventMarketId]));

                                    $memUID = $masterEventMarketsTable->get($masterEventMarketId, 'master_event_market_unique_id');
                                    $masterEventMarketsTable->del($masterEventMarketId);
                                    // $masterEventMarketsIndexTable->del($memUID);
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
                    }
                }
            }
        } catch (Exception $e) {
            logger('error', 'odds', $e, $message);
        }
    }
}
