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
}