<?php

namespace Models;

use Models\Model;

class Team extends Model
{
    protected static $table = 'teams';

    public static function getActiveTeams($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }

    public static function getTeam($connection, $teamId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE id = $teamId";
        return $connection->query($sql);
    }

    public static function getUnmatchedTeams($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " as t
                WHERE NOT EXISTS (SELECT null FROM team_groups as tg WHERE tg.team_id = t.id)";
        return $connection->query($sql);
    }
}