<?php

namespace Models;

class ProviderBet
{
    protected static $table = 'provider_bets';

    public static function getDataByBetId($connection, $providerBetId, bool $withProviderBetTransactions = false)
    {
        $select = "";
        $join   = "";

        if ($withProviderBetTransactions) {
            $select .= ", pb.id AS provider_bet_log_id, pbt.exchange_rate AS exchange_rate, pbt.actual_stake AS astake, pbt.actual_to_win AS ato_win";
            $join   .= "LEFT JOIN provider_bet_logs AS pbl ON pbl.provider_bet_id = pb.id AND pbl.status = 'SUCCESS'
                LEFT JOIN provider_bet_transactions AS pbt ON pbt.provider_bet_id = pb.id";
        }

        $sql = "SELECT pb.*, ub.odd_type_id, u.currency_id, u.uuid, ub.user_id, c.code{$select}
            FROM " . self::$table . " as pb
            JOIN user_bets as ub ON pb.user_bet_id = ub.id
            JOIN users as u ON u.id = ub.user_id
            JOIN currency as c ON c.id = u.currency_id
            {$join}
            WHERE pb.bet_id LIKE '%{$providerBetId}'
            ORDER BY pb.id
            LIMIT 1";

        return $connection->query($sql);
    }

    public static function getActiveProviderBets($connection)
    {
        $sql = "SELECT pb.id, pb.status, pb.created_at, pb.bet_id, ub.order_expiry, pa.username, u.currency_id as user_currency_id, ub.user_id FROM " . self::$table . " as pb
            JOIN user_bets as ub ON pb.user_bet_id = ub.id
            JOIN provider_accounts as pa ON pa.id = pb.provider_account_id
            JOIN users as u ON u.id = ub.user_id
            WHERE pb.settled_date is null";
        return $connection->query($sql);
    }

    public static function updateByBetIdNumber($connection, $providerBetId, $arrayParams)
    {
        $sql    = "UPDATE " . self::$table . " SET ";
        $params = [];
        foreach ($arrayParams as $key => $value) {
            $params[] = "{$key} = '{$value}'";
        }
        $sql .= implode(', ', $params);
        $sql .= "WHERE bet_id LIKE '%{$providerBetId}'";
        return $connection->query($sql);
    }

    public static function getUnsettledDates($connection, $providerAccountId)
    {
        $sql = "SELECT DISTINCT DATE(pb.created_at) as unsettled_date, pa.username,
                    pb.provider_account_id FROM " . self::$table . " pb JOIN
                    provider_accounts pa ON pa.id = pb.provider_account_id
                    WHERE pb.settled_date IS NULL AND pb.provider_account_id = '{$providerAccountId}'
                    GROUP BY pa.username, pb.created_at, pb.provider_account_id";
        return $connection->query($sql);
    }
}