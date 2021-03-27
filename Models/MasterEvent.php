<?php

namespace Models;

use Models\Model;

class MasterEvent extends Model
{
    protected static $table = 'master_events';

    public static function getMasterEventData($connection, $masterLeagueId, $masterHomeTeamId, $masterAwayTeamId, $refSchedule)
    {
        $sql = "SELECT " . static::$table . ".* FROM " . static::$table 
                . " JOIN event_groups ON " . static::$table . ".id = event_groups.master_event_id"
                . " JOIN events ON event_groups.event_id = events.id"
                . " WHERE master_league_id = '{$masterLeagueId}'"
                . " AND master_team_home_id = '{$masterHomeTeamId}'"
                . " AND master_team_away_id = '{$masterAwayTeamId}'" 
                . " AND events.ref_schedule = '{$refSchedule}'"
                . " AND " . static::$table . ".deleted_at IS NULL ORDER BY events.created_at DESC LIMIT 1";
        return $connection->query($sql);
    }

    public static function checkIfExists($connection, $masterEventUniqueId)
    {
        $sql = "SELECT " . static::$table . " FROM " . static::$table . " WHERE master_event_unique_id = '{$masterEventUniqueId}'";
        return $connection->query($sql);
    }
}