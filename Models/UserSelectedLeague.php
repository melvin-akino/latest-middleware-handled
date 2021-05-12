<?php

namespace Models;

use Models\Model;

class UserSelectedLeague extends Model
{
    protected static $table = 'user_selected_leagues';

    public static function getUserSelectedLeagues($connection)
    {
        $masterLeagueIds = [];
        $masterEventIds = [];

        $sql = "SELECT master_league_id FROM " . self::$table;
        $userLeagues = $connection->query($sql);

        if ($userLeagues) {            
            $masterLeagueIdArray = $connection->fetchAll($userLeagues);
            foreach($masterLeagueIdArray as $league) {
                $masterLeagueIds[] = $league['master_league_id'];
            }
            $masterLeagueIdList = implode(",",$masterLeagueIds);
            $masterEvents = MasterEvent::getMasterEventIdByMasterLeagueId($connection, $masterLeagueIdList);

            if ($masterEvents) {                
                $masterEventIdArray = $connection->fetchAll($masterEvents);
                foreach($masterEventIdArray as $event) {
                    $masterEventIds[] = $event['id'];
                }
            }
        }

        return $masterEventIds;
    }
}

