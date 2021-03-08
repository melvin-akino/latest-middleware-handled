<?php

namespace Models;

Class ProviderAccountOrder
{
    private static $table = 'provider_account_orders';

    public static function create($connection, $params)
    {
		$sql = "INSERT INTO " . self::$table . "(";
		$array2 = self::_index2string($params);

		$sql .= self::_field($array2);

		$sql .= ") VALUES (";
		
        $sql .= self::_sfield($params);
		
        $sql .= ")";
        echo $sql . "\n";
		return $connection->query($sql);
	}

	private function _field($array)
	{
		$str = implode(", ", $array);
		return $str;
	}

    private function _sfield($array)
    {
		$str = "";
		for ($i = 0; $i < count($array); $i++){
			$str .= "'" . addslashes($array[key($array)]) . "'";
			next($array);
			if ($i < count($array) - 1) {
				$str .= ", ";
			}
		}
		return $str;
	}

    private function _index2string($array)
    {
		$array2 = array();
		for ($i=0; $i < count($array); $i++) {
			$array2[key($array)] = key($array);
			next($array);
		}
		return $array2;
	}
}