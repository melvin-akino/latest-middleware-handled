<?php

namespace Models;

class SystemConfiguration
{
    private static $table = 'system_configurations';

    public static function getProviderMaintenanceConfigData($connection)
    {
        $sql = "SELECT type, value FROM " . self::$table . " WHERE type LIKE '%_MAINTENANCE'";
        return $connection->query($sql);
    }

    public static function getPrimaryProvider($connection)
    {
        $sql = "SELECT type, value FROM " . self::$table . " WHERE type = 'PRIMARY_PROVIDER'";
        return $connection->query($sql);
    }

    public static function getMaxMissingCount4Deletion($connection, $schedule)
    {
        $sql = "SELECT type, value FROM " . self::$table . " WHERE type = '" . strtoupper($schedule) . "_MISSING_MAX_COUNT_FOR_DELETION'";
        return $connection->query($sql);
    }
}