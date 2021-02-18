<?php

namespace Models;

Class ProviderAccount
{
    private static $table = 'provider_accounts';

    public static function getEnabledProviderAccounts($connection)
    {
        $sql = "SELECT pa.*, p.alias FROM " . self::$table . " as pa LEFT JOIN providers as p ON p.id = pa.provider_id
                WHERE pa.deleted_at is null AND pa.is_idle = true AND p.is_enabled = true";
        return $connection->query($sql);
        
    }

    public static function updateBalance($connection, string $username, int $providerId, $credits)
    {
        $sql = "UPDATE " . self::$table . " SET credits = '{$credits}' WHERE username = '{$username}' AND provider_id = '{$providerId}'";
        return $connection->query($sql);
    }
}