<?php

namespace Models;

use Models\Model;

class MasterEvent extends Model
{
    protected static $table = 'master_events';

    public static function getMasterEventData($connection, $masterLeagueId, $masterHomeTeamId, $masterAwayTeamId)
    {
        $sql = "SELECT * FROM " . static::$table 
                . " JOIN event_groups ON " . static::$table . ".id = event_groups.master_event_id"
                . " JOIN events ON event_groups.event_id = events.id"
                . " WHERE master_league_id = '{$masterLeagueId}' AND master_home_team_id = '{$masterHomeTeamId}' AND master_away_team_id = '{$masterAwayTeamId}' AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1";
        return $connection->query($sql);
    }
}