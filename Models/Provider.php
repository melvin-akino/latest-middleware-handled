<?php

namespace Models;

Class Provider
{
    private static $table = 'providers';

    public static function getActiveProviders($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE is_enabled = true";
        return $connection->query($sql);
        
    }
}