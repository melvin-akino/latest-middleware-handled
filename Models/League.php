<?php

namespace Models;

use Models\Model;

class League extends Model
{
    protected static $table = 'leagues';

    public static function getActiveLeagues($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }

    public static function getLeague($connection, $leagueId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE id = $leagueId";
        return $connection->query($sql);
    }

    public static function getUnmatchedLeagues($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " as l
                WHERE NOT EXISTS (SELECT null FROM league_groups as lg WHERE lg.league_id = l.id)";
        return $connection->query($sql);
    }
}