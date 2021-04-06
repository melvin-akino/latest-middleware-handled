<?php

namespace Workers;

use Models\{League, Team, Event, UnmatchedData as UnmatchedDataModel};
use Carbon\Carbon;
use Co\System;
use Exception;

class UnmatchedData
{
    public static function handle($dbPool, $swooleTable) 
    {
        while (true) {
            logger('info', 'unmatch', "Looking for unmatched data");

            $connection = $dbPool->borrow();

            try {
                $unmatchedLeagueResult = League::getUnmatchedLeagues($connection);
                $unmatchedLeagues = $connection->fetchAll($unmatchedLeagueResult);
                if (is_array($unmatchedLeagues)) {
                    foreach ($unmatchedLeagues as $league) {
                        $key = implode(':', [
                            'pId:' . $league['provider_id'],
                            'name:' . md5($league['name']),
                        ]);
    
                        if (!$swooleTable['unmatchedLeagues']->exists($key)) {
                            UnmatchedDataModel::create($connection, [
                                'data_type' =>   'league',
                                'data_id'   =>   $league['id'],
                                'provider_id' => $league['provider_id']
                            ]);
    
                            $swooleTable['unmatchedLeagues']->set($key, [
                                'id'          => $league['id'],
                                'name'        => $league['name'],
                                'sport_id'    => $league['sport_id'],
                                'provider_id' => $league['provider_id'],
                            ]);
                        }
                    }
                }

                $unmatchedTeamResult = Team::getUnmatchedTeams($connection);
                $unmatchedTeams = $connection->fetchAll($unmatchedTeamResult);
                if (is_array($unmatchedTeams)) {
                    foreach ($unmatchedTeams as $team) {
                        $key = implode(':', [
                            'pId:' . $team['provider_id'],
                            'name:' . md5($team['name']),
                        ]);

                        if (!$swooleTable['unmatchedTeams']->exists($key)) {
                            UnmatchedDataModel::create($connection, [
                                'data_type' =>   'team',
                                'data_id'   =>   $team['id'],
                                'provider_id' => $team['provider_id']
                            ]);

                            $swooleTable['unmatchedTeams']->set($key, [
                                'id'          => $team['id'],
                                'name'        => $team['name'],
                                'sport_id'    => $team['sport_id'],
                                'provider_id' => $team['provider_id'],
                            ]);
                        }
                    }
                }


                $unmatchedEventResult = Event::getUnmatchedEvents($connection);
                $unmatchedEvents = $connection->fetchAll($unmatchedEventResult);
                if (is_array($unmatchedEvents)) {
                    foreach ($unmatchedEvents as $event) {
                        $key = implode(':', [
                            'pId:' . $event['provider_id'],
                            'event_identifier:' . $event['event_identifier'],
                        ]);

                        if (!$swooleTable['unmatchedEvents']->exists($key)) {
                            UnmatchedDataModel::create($connection, [
                                'data_type' =>   'event',
                                'data_id'   =>   $event['id'],
                                'provider_id' => $event['provider_id']
                            ]);

                            $swooleTable['unmatchedEvents']->set($key, [
                                'id'               => $event['id'],
                                'event_identifier' => $event['event_identifier'],
                                'sport_id'         => $event['sport_id'],
                                'provider_id'      => $event['provider_id'],
                            ]);
                        }
                    }
                }
            } catch (Exception $e) {
                logger('error', 'matching', "Something went wrong", (array) $e);
            }

            $dbPool->return($connection);
            System::sleep(10);
        }
    }
}
