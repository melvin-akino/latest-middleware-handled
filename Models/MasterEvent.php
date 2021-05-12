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
                . " AND events.deleted_at IS NULL"
                . " AND " . static::$table . ".deleted_at IS NULL ORDER BY events.created_at DESC LIMIT 1";
        return $connection->query($sql);
    }

    public static function checkIfExists($connection, $masterEventUniqueId)
    {
        $sql = "SELECT " . static::$table . " FROM " . static::$table . " WHERE master_event_unique_id = '{$masterEventUniqueId}'";
        return $connection->query($sql);
    }

    public static function checkIfHasMaster($connection, $masterEventUniqueId)
    {
        $sql = "SELECT * FROM " . static::$table . "
                WHERE master_event_unique_id = '{$masterEventUniqueId}' LIMIT 1";
        return $connection->query($sql);
    }

    public static function getMasterEventIdByMasterLeagueId($connection, $providerId, $schedule, $masterLeagueIds)
    {
        $sql = "SELECT master_events.id FROM " . static::$table 
            ." JOIN event_groups ON event_groups.master_event_id=". static::$table . ".id "
            ." JOIN events ON events.id=event_groups.event_id"
            ." WHERE master_league_id in ($masterLeagueIds) 
                AND events.provider_id={$providerId}
                AND game_schedule = '{$schedule}'
                AND events.deleted_at is null";
        return $connection->query($sql);
    }
}