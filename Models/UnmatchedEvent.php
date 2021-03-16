<?php

namespace Models;

use Models\Model;

class UnmatchedEvent extends Model
{
    protected static $table = 'unmatched_data';

    public static function getAllUnmatchedEvents($connection)
    {
        $sql = "SELECT * FROM " . static::$table
            . " JOIN events on " . static::$table . ".data_id = events.id"
            . " WHERE type = 'event'";
        return $connection->query($sql);
    }

    public static function deleteUnmatched($connection, $dataId)
    {
        $sql = "DELETE FROM " . static::$table . " WHERE type = 'event' AND data_id = {$dataId}";
        return $connection->query($sql);
    }
}