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
    MasterEventMarket,
    UnmatchedData
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
            lockProcess($leagueIndexHash, 'league');
            if ($leaguesTable->exists($leagueIndexHash)) {
                $leagueId = $leaguesTable[$leagueIndexHash]['id'];
                logger('info', 'odds', 'league ID from $leaguesTable ' . $leagueId);
            } else {
                $swooleTable['lockHashData'][$leagueIndexHash]['type'] = 'league';
                try {
                    $leagueResult = League::create($connection, [
                        'sport_id'    => $sportId,
                        'provider_id' => $providerId,
                        'name'        => $leagueName,
                        'created_at'  => $timestamp
                    ], 'id');
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the league');
                    return;
                }

                $league   = $connection->fetchArray($leagueResult);
                $leagueId = $league['id'];

                logger('info', 'odds', 'League Created ' . $leagueId, [
                    'sport_id'    => $sportId,
                    'provider_id' => $providerId,
                    'name'        => $leagueName
                ]);

                $leaguesTable[$leagueIndexHash] = [
                    'id'          => $leagueId,
                    'sport_id'    => $sportId,
                    'provider_id' => $providerId,
                    'name'        => $leagueName,
                ];

                if (strtolower($primaryProvider['value']) != strtolower($provider)) {
                    UnmatchedData::create($connection, [
                        'provider_id' => $providerId,
                        'data_type' => 'league',
                        'data_id' => $leagueId
                    ]);
                }
            }

            $swooleTable['lockHashData']->del($leagueIndexHash);
            /** end league **/

            /** master league **/
            $masterLeagueId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $leagueGroupResult = LeagueGroup::checkIfMatched($connection, $leagueId);
                $leagueGroupData   = $connection->fetchArray($leagueGroupResult);

                if (!$leagueGroupData) {
                    try {
                        $masterLeagueResult = MasterLeague::create($connection, [
                            'sport_id'   => $sportId,
                            'name'       => null,
                            'created_at' => $timestamp
                        ], 'id');
                        $masterLeague       = $connection->fetchArray($masterLeagueResult);
                        $masterLeagueId = $masterLeague['id'];
                        logger('info', 'odds', 'Master League Created ' . $masterLeagueId, [
                            'sport_id'   => $sportId,
                            'name'       => null,
                            'created_at' => $timestamp
                        ]);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the master league');
                        return;
                    }

                    try {
                        LeagueGroup::create($connection, [
                            'league_id'        => $leagueId,
                            'master_league_id' => $masterLeagueId
                        ]);
                        logger('info', 'odds', 'League Group Created', [
                            'league_id'        => $leagueId,
                            'master_league_id' => $masterLeagueId
                        ]);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the league group');
                        return;
                    }
                } else {
                    $masterLeagueId = $leagueGroupData['master_league_id'];
                }
            }


            /** home team **/
            /** teams **/
            $teamHomeId    = null;
            $teamIndexHash = md5($homeTeam . ':' . $sportId . ':' . $providerId);
            lockProcess($teamIndexHash, 'team');
            if ($teamsTable->exists($teamIndexHash)) {
                $teamHomeId = $teamsTable[$teamIndexHash]['id'];
                logger('info', 'odds', 'team ID from $teamsTable ' . $teamHomeId);
            } else {
                $swooleTable['lockHashData'][$teamIndexHash]['type'] = 'team';
                try {
                    $teamResult = Team::create($connection, [
                        'provider_id' => $providerId,
                        'name'        => $homeTeam,
                        'sport_id'    => $sportId,
                        'created_at'  => $timestamp
                    ], 'id');
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the team');
                    return;
                }

                $team       = $connection->fetchArray($teamResult);
                $teamHomeId = $team['id'];

                logger('info', 'odds', 'Team Created ' . $teamHomeId, [
                    'provider_id' => $providerId,
                    'name'        => $homeTeam,
                    'sport_id'    => $sportId
                ]);

                $teamsTable[$teamIndexHash] = [
                    'id'          => $teamHomeId,
                    'provider_id' => $providerId,
                    'name'        => $homeTeam,
                    'sport_id'    => $sportId,
                ];

                if (strtolower($primaryProvider['value']) != strtolower($provider)) {
                    UnmatchedData::create($connection, [
                        'provider_id' => $providerId,
                        'data_type' => 'team',
                        'data_id' => $teamHomeId
                    ]);
                }
            }

            $swooleTable['lockHashData']->del($teamIndexHash);
            /** end home team **/

            /** master teams **/
            $masterTeamHomeId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $teamGroupResult = TeamGroup::checkIfMatched($connection, $teamHomeId);
                $teamGroupData   = $connection->fetchArray($teamGroupResult);

                if (!$teamGroupData) {
                    try {
                        $masterTeamResult = MasterTeam::create($connection, [
                            'sport_id'   => $sportId,
                            'name'       => null,
                            'created_at' => $timestamp
                        ], 'id');
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

                        logger('info', 'odds', 'Team Groups Created', [
                            'team_id'        => $teamHomeId,
                            'master_team_id' => $masterTeamHomeId
                        ]);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the team group');
                        return;
                    }
                } else {
                    $masterTeamHomeId = $teamGroupData['master_team_id'];
                }
            }

            /** away team **/
            /** teams **/
            $teamAwayId    = null;
            $teamIndexHash = md5($awayTeam . ':' . $sportId . ':' . $providerId);
            lockProcess($teamIndexHash, 'team');
            if ($teamsTable->exists($teamIndexHash)) {
                $teamAwayId = $teamsTable[$teamIndexHash]['id'];
                logger('info', 'odds', 'team ID from $teamsTable ' . $teamAwayId);
            } else {
                $swooleTable['lockHashData'][$teamIndexHash]['type'] = 'team';
                try {
                    $teamResult = Team::create($connection, [
                        'provider_id' => $providerId,
                        'name'        => $awayTeam,
                        'sport_id'    => $sportId,
                        'created_at'  => $timestamp
                    ], 'id');
                } catch (Exception $e) {
                    logger('error', 'odds', 'Another worker already created the team');
                    return;
                }

                $team       = $connection->fetchArray($teamResult);
                $teamAwayId = $team['id'];

                logger('info', 'odds', 'Team Created ' . $teamAwayId, [
                    'provider_id' => $providerId,
                    'name'        => $awayTeam,
                    'sport_id'    => $sportId,
                ]);

                $teamsTable[$teamIndexHash] = [
                    'id'          => $teamAwayId,
                    'provider_id' => $providerId,
                    'name'        => $awayTeam,
                    'sport_id'    => $sportId,
                ];

                if (strtolower($primaryProvider['value']) != strtolower($provider)) {
                    UnmatchedData::create($connection, [
                        'provider_id' => $providerId,
                        'data_type' => 'team',
                        'data_id' => $teamAwayId
                    ]);
                }
            }

            $swooleTable['lockHashData']->del($teamIndexHash);
            /** end master away team **/

            /** master away teams **/
            $masterTeamAwayId = null;
            if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                $teamGroupResult = TeamGroup::checkIfMatched($connection, $teamAwayId);
                $teamGroupData   = $connection->fetchArray($teamGroupResult);

                if (!$teamGroupData) {
                    try {
                        $masterTeamResult = MasterTeam::create($connection, [
                            'sport_id'   => $sportId,
                            'name'       => null,
                            'created_at' => $timestamp
                        ], 'id');

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

                        logger('info', 'odds', 'Team Groups Created', [
                            'team_id'        => $teamAwayId,
                            'master_team_id' => $masterTeamAwayId
                        ]);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the team group');
                        return;
                    }
                } else {
                    $masterTeamAwayId = $teamGroupData['master_team_id'];
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

                logger('info', 'odds', 'Event Updated event identifier ' . $eventIdentifier, [
                    'ref_schedule'  => $referenceSchedule,
                    'missing_count' => $missingCount,
                    'game_schedule' => $gameSchedule,
                    'score'         => $score,
                    'running_time'  => $runningtime,
                    'home_penalty'  => $homeRedcard,
                    'away_penalty'  => $awayRedcard,
                    'deleted_at'    => null,
                    'updated_at'    => $timestamp
                ]);
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

                        logger('info', 'odds', 'Event Updated event identifier ' . $eventIdentifier, [
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
                            'away_penalty'  => $awayRedcard
                        ]);
                    } else {
                        $eventResult = Event::create($connection, [
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
                        ], 'id');

                        $event       = $connection->fetchArray($eventResult);

                        logger('info', 'odds', 'Event Created ' . $event['id'], [
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
                            'away_penalty'     => $awayRedcard
                        ]);

                        if (strtolower($primaryProvider['value']) != strtolower($provider)) {
                            UnmatchedData::create($connection, [
                                'provider_id' => $providerId,
                                'data_type' => 'event',
                                'data_id' => $event['id']
                            ]);
                        }
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

            if (empty($eventId)) {
                logger('error', 'odds', 'Event ID is empty', $message);
                return;
            }
            /* end events */

            $isEventMatched = false;
            $masterEventId = null;
            $eventGroupResult = EventGroup::checkIfMatched($connection, $eventId);
            $eventGroupData   = $connection->fetchArray($eventGroupResult);
            if (!$eventGroupData) {
                if (strtolower($primaryProvider['value']) == strtolower($provider)) {
                    $masterEventUniqueId = date('Ymd', strtotime($referenceSchedule)) . '-' . $sportId . '-' . $masterLeagueId . '-' . $eventIdentifier;
                    try {

                        $masterEventResult = MasterEvent::checkIfExists($connection, $masterEventUniqueId);
                        if ($connection->numRows($masterEventResult)) {
                            $masterEvent       = $connection->fetchArray($masterEventResult);

                            $masterEventResult = MasterEvent::update($connection, [
                                'sport_id'               => $sportId,
                                'master_league_id'       => $masterLeagueId,
                                'master_team_home_id'    => $masterTeamHomeId,
                                'master_team_away_id'    => $masterTeamAwayId,
                                'updated_at'             => $timestamp
                            ], [
                                'id' => $masterEvent['id']
                            ]);
                        } else {
                            $masterEventResult = MasterEvent::create($connection, [
                                'sport_id'               => $sportId,
                                'master_event_unique_id' => $masterEventUniqueId,
                                'master_league_id'       => $masterLeagueId,
                                'master_team_home_id'    => $masterTeamHomeId,
                                'master_team_away_id'    => $masterTeamAwayId,
                                'created_at'             => $timestamp
                            ], 'id');
                            $masterEvent       = $connection->fetchArray($masterEventResult);
                        }

                        $masterEventId = $masterEvent['id'];

                        logger('info', 'odds', 'Master Event Created ' . $masterEventId, [
                            'sport_id'               => $sportId,
                            'master_event_unique_id' => $masterEventUniqueId,
                            'master_league_id'       => $masterLeagueId,
                            'master_team_home_id'    => $masterTeamHomeId,
                            'master_team_away_id'    => $masterTeamAwayId
                        ]);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the master event');
                        return;
                    }

                    try {
                        EventGroup::create($connection, [
                            'event_id'        => $eventId,
                            'master_event_id' => $masterEventId,
                        ]);

                        logger('info', 'odds', 'Event Groups Created', [
                            'event_id'        => $eventId,
                            'master_event_id' => $masterEventId
                        ]);
                    } catch (Exception $e) {
                        logger('error', 'odds', 'Another worker already created the event group');
                        return;
                    }

                    $isEventMatched = true;
                } 
            } else {
                $isEventMatched = true;
                $masterEventId = $eventGroupData['master_event_id'];
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
                                    $eventMarketsTable->del(md5(implode(':', [$providerId, $marketId])));
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
                                        $eventMarket = $eventMarketsTable[md5(implode(':', [$providerId, $activeEventMarket]))];
                                        if (
                                            $eventMarket['odd_type_id'] == $oddTypeId &&
                                            $eventMarket['provider_id'] == $providerId &&
                                            $eventMarket['market_event_identifier'] == $event["eventId"] &&
                                            $eventMarket['market_flag'] == $marketFlag
                                        ) {
                                            EventMarket::softDelete($connection, 'bet_identifier', $activeEventMarket);
                                            $eventMarketsTable->del(md5(implode(':', [$providerId, $activeEventMarket])));

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

                                            logger('info', 'odds', 'Event Market Update event market ID ' . $eventMarketId, [
                                                'odds'                    => $odds,
                                                'odd_label'               => $points,
                                                'is_main'                 => $marketType,
                                                'market_flag'             => $marketFlag,
                                                'event_id'                => $eventId,
                                                'market_event_identifier' => $event["eventId"]
                                            ]);
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

                                            logger('info', 'odds', 'Event Market Update event market ID ' . $eventMarketId, [
                                                'event_id'                => $eventId,
                                                'odd_type_id'             => $oddTypeId,
                                                'odds'                    => $odds,
                                                'odd_label'               => $points,
                                                'bet_identifier'          => $marketId,
                                                'is_main'                 => $marketType,
                                                'market_flag'             => $marketFlag,
                                                'provider_id'             => $providerId,
                                                'market_event_identifier' => $event["eventId"]
                                            ]);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already updated the event market ' . $eventMarketId, [
                                                'event_id'                => $eventId,
                                                'odd_type_id'             => $oddTypeId,
                                                'odds'                    => $odds,
                                                'odd_label'               => $points,
                                                'bet_identifier'          => $marketId,
                                                'is_main'                 => $marketType,
                                                'market_flag'             => $marketFlag,
                                                'provider_id'             => $providerId,
                                                'market_event_identifier' => $event["eventId"]
                                            ]);
                                            return;
                                        }
                                    } else {
                                        try {
                                            $eventMarketResult = EventMarket::create($connection, [
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
                                            ], 'id');
                                            $eventMarket       = $connection->fetchArray($eventMarketResult);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already created the event market', [
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
                                            return;
                                        }
                                        $eventMarketId = $eventMarket['id'];

                                        logger('info', 'odds', 'Event Market Created ' . $eventMarketId, [
                                            'event_id'                => $eventId,
                                            'odd_type_id'             => $oddTypeId,
                                            'odds'                    => $odds,
                                            'odd_label'               => $points,
                                            'bet_identifier'          => $marketId,
                                            'is_main'                 => $marketType,
                                            'market_flag'             => $marketFlag,
                                            'provider_id'             => $providerId,
                                            'market_event_identifier' => $event["eventId"]
                                        ]);
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

                                if ($eventMarket['odd_label'] != $points) {
                                    EventMarketGroup::delete($connection, 'event_market_id', $eventMarketId);
                                }

                                $newMarkets[] = $marketId;
                                $eventMarketListTable->set($eventId, ['marketIDs' => implode(',', $newMarkets)]);

                                if (empty($masterEventId)) {
                                    continue;
                                }

                                $masterEventMarketId = null;
                                if ($isEventMatched) {
                                    $memUID                 = md5($eventId . strtoupper($marketFlag) . $marketId);
                                    $eventMarketGroupResult = EventMarketGroup::checkIfMatched($connection, $eventMarketId);
                                    $eventMarketGroupData   = $connection->fetchArray($eventMarketGroupResult);
                                    if (!$eventMarketGroupData && (strtolower($primaryProvider['value']) == strtolower($provider))) {
                                        try {
                                            $masterEventMarketResult = MasterEventMarket::checkIfMemUIDExists($connection, $memUID);
                                            if ($connection->numRows($masterEventMarketResult)) {
                                                $masterEventMarket = $connection->fetchArray($masterEventMarketResult);
                                            } else {
                                                $masterEventMarketResult = MasterEventMarket::create($connection, [
                                                    'master_event_id'               => $masterEventId,
                                                    'master_event_market_unique_id' => $memUID,
                                                    'created_at'                    => $timestamp
                                                ], 'id');

                                                $masterEventMarket       = $connection->fetchArray($masterEventMarketResult);
                                            }
                                            $masterEventMarketId = $masterEventMarket['id'];

                                            logger('info', 'odds', 'Master Event Market Created ' . $masterEventMarketId, [
                                                'master_event_id'               => $masterEventId,
                                                'master_event_market_unique_id' => $memUID
                                            ]);
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

                                            logger('info', 'odds', 'Event Market Groups Created', [
                                                'event_market_id'        => $eventMarketId,
                                                'master_event_market_id' => $masterEventMarketId
                                            ]);
                                        } catch (Exception $e) {
                                            logger('error', 'odds', 'Another worker already created the event market group');
                                            return;
                                        }
                                    } else {
                                        $masterEventMarketId = $eventMarketGroupData['master_event_market_id'];
                                    }
                                }
                            } else {
                                if (is_array($activeEventMarkets) && in_array($marketId, $activeEventMarkets)) {
                                    EventMarket::softDelete($connection, 'bet_identifier', $marketId);

                                    $eventMarketsTable->del(md5(implode(':', [$providerId, $marketId])));

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
                        $eventMarketsTable->del(md5(implode(':', [$providerId, $activeEventMarket])));

                        logger('info', 'odds', 'Event Market Deleted with bet identifier ' . $activeEventMarket);
                    }
                }
            }
        } catch (Exception $e) {
            logger('error', 'odds', $e, $message);
        }
    }
}