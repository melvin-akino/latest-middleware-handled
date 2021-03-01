<?php

namespace Models;

use Models\Model;

Class TeamGroup extends Model
{
    protected static $table = 'team_groups';

    public static function getTeamsData($connection)
    {
        $sql = "SELECT tg.*, name, sport_id, provider_id FROM " . self::$table . " as tg
                JOIN teams as t ON tg.team_id = t.id WHERE t.deleted_at is null";
        return $connection->query($sql);
    }
}