<?php

namespace Models;

use Models\Model;

class TeamGroup extends Model
{
    protected static $table = 'team_groups';

    public static function checkIfMatched($connection, $teamId)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE team_id = '{$teamId}'";
        return $connection->query($sql);
    }
}