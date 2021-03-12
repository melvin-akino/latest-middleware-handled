<?php

namespace Models;

use Models\Model;

class MasterEvent extends Model
{
    protected static $table = 'master_events';

    public static function checkIfIdExists($connection, $masterLeagueId, $masterHomeTeamId, $masterAwayTeamId)
    {
        $sql = "SELECT * FROM " . static::$table . " WHERE master_league_id = '{$masterLeagueId}' AND master_home_team_id = '{$masterHomeTeamId}' AND master_away_team_id = '{$masterAwayTeamId}' ORDER BY created_at DESC LIMIT 1";
        return $connection->query($sql);
    }
}