<?php
namespace M1ke\Sql;

use Aura\Sql\ExtendedPdo as AuraPdo;
use Aura\Sql\ExtendedPdoInterface;
use Aura\Sql\Exception;

/**
 *
 * Expands Aura's ExtendedPdo to add database manipulation helpers
 *
 * @package M1ke.Sql
 *
 */
class ExtendedPdo extends AuraPdo implements ExtendedPdoInterface
{
	public function queryInsert($db_name, Array $params, Array $exclude_keys = []){
		$query = "INSERT INTO `$db_name` ".self::makeQueryInsert($params, $exclude_keys);
		return $this->perform($query, $params);
	}

	public function queryUpdate($query, Array $params, Array $exclude_keys = []){
		$query_update = self::makeQueryInsert($params, $exclude_keys);
		$query = str_replace("SET ", "SET $query_update", $query);
		return $this->perform($query, $params);
	}

	public static function makeQueryParams($type, Array $params, Array $exclude_keys = []){
		foreach ($params as $key => $val){
			$value_isnt_false = $val!==false;
			$key_has_no_dash = !in_string('-', $key);
			$key_isnt_excluded = !in_array($key, $exclude_keys);
			$value_isnt_array = !is_array($val);
			if ($value_isnt_false && $key_has_no_dash && $key_isnt_excluded && $value_isnt_array){
				$val = is_null($val) ? "''" : ":$key";
				$fields[] = $type==='update' ? "`$key`=$val" : "`$key`";
				$vals[] = $val;
			}
		}
		return [$fields, $vals];
	}

	private static function makeQueryInsert(Array $params, Array $exclude_keys = []){
		list($fields, $vals) = self::makeQueryParams('insert', $params, $exclude_keys);
		$vals = "(".implode(',', $vals).")";
		return "(".implode(',', $fields).") VALUES $vals";
	}

	private static function makeQueryUpdate(Array $params, Array $exclude_keys = []){
		list($query) = self::makeQueryParams('update', $params, $exclude_keys);
		return implode(',', $query);
	}
}
