<?php

namespace Models;

use Models\Model;

Class MasterLeague extends Model
{
    protected static $table = 'master_leagues';

    public static function getAll($connection)
    {
        $sql = "SELECT * FROM " . self::$table;
        return $connection->query($sql);
    }

    public static function getDataBySportAndLeagueName($connection, $sportId, $leagueName)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE sport_id = '{$sportId}' AND name = '$leagueName'";
        return $connection->query($sql);
    }
}