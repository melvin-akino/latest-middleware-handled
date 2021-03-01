<?php

namespace Models;

use Models\Model;

Class EventGroup extends Model
{
    protected static $table = 'event_groups';

    public static function getEventsData($connection)
    {
        $sql = "SELECT eg.*, event_identifier, sport_id, provider_id FROM " . self::$table . " as eg
                JOIN events as e ON eg.event_id = e.id WHERE e.deleted_at is null";
        return $connection->query($sql);
    } 
}