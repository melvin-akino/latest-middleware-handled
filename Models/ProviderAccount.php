<?php

namespace Models;

Class ProviderAccount
{
    private static $table = 'provider_accounts';

    public static function getAll($connection)
    {
        $sql = "SELECT * FROM " . self::$table . " WHERE deleted_at is null";
        return $connection->query($sql);
    }

    public static function getEnabledProviderAccounts($connection)
    {
        $sql = "SELECT
                pa.*,
                p.alias
            FROM " . self::$table . " as pa

            LEFT JOIN providers as p
                ON p.id = pa.provider_id

            WHERE pa.deleted_at is null
            AND pa.is_idle = true
            AND pa.is_enabled = true
            AND p.is_enabled = true
            AND pa.deleted_at is null";

        return $connection->query($sql);
    }

    public static function updateBalance($connection, string $username, int $providerId, $credits)
    {
        $sql = "UPDATE " . self::$table . " SET credits = '{$credits}' WHERE username = '{$username}' AND provider_id = '{$providerId}'";
        return $connection->query($sql);
    }

    public static function getByProviderAndTypes($connection, $providerId, $providerTypes)
    {
        $sql = "SELECT username, password, type, is_enabled FROM " . self::$table . " WHERE provider_id = '{$providerId}' AND type IN ('" . implode("', '", array_keys($providerTypes)) . "') AND deleted_at is null";
        return $connection->query($sql);
    }

    public static function updateToActive($connection, $providerAccountId)
    {
        $sql = "UPDATE " . self::$table . " SET deleted_at = null, is_idle = true, is_enabled = true WHERE id = '{$providerAccountId}'";
        return $connection->query($sql);
    }

    public static function updateToInactive($connection, $providerAccountId)
    {
        $sql = "UPDATE " . self::$table . " SET deleted_at = NOW(), is_idle = false, is_enabled = false WHERE id = '{$providerAccountId}'";
        return $connection->query($sql);
    }
}