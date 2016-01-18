<?php
namespace M1ke\Sql;

use Aura\Sql\ExtendedPdo as AuraPdo;
use Aura\Sql\ExtendedPdoInterface;

use PDOException;

/**
 *
 * Expands Aura's ExtendedPdo to add database manipulation helpers
 *
 * @package M1ke.Sql
 *
 */
class ExtendedPdo extends AuraPdo implements ExtendedPdoInterface {
	public static $where_key_collision = '____';

	/**
	 * ExtendedPdo constructor.
	 * @param string $db
	 * @param string $user
	 * @param string $pass
	 * @param array $options
	 * @param string $type
	 * @param string $server
	 */
	public function __construct($db, $user, $pass, array $options = [], $type = 'mysql', $server = 'localhost'){
		$dsn = $type . ':host=' . $server . ';dbname=' . $db;
		parent::__construct($dsn, $user, $pass, $options);
	}

	/**
	 * @param array $values
	 * @param array $exclude_keys
	 * @return array
	 */
	public static function excludeKeys(array $values, array $exclude_keys){
		$include_keys = array_keys($values);
		foreach ($include_keys as $n => $key){
			if (in_array($key, $exclude_keys)){
				unset($include_keys[$n]);
			}
		}

		return $include_keys;
	}

	/**
	 * @param string $fetch_type
	 * @param string $statement
	 * @param array $values
	 * @param null $callable
	 * @return array
	 */
	protected function fetchAllWithCallable($fetch_type, $statement, array $values = [], $callable = null){
		$args = func_get_args();
		$return = parent::fetchAllWithCallable(...$args);

		return is_array($return) ? $return : [];
	}

	/**
	 * @param string $statement
	 * @param array $values
	 * @return array
	 */
	public function fetchOne($statement, array $values = []){
		$args = func_get_args();
		$return = parent::fetchOne(...$args);

		return is_array($return) ? $return : [];
	}

	/**
	 *
	 * Fetches an associative array of rows from the database; the rows
	 * are returned as associative arrays, and the array of rows is keyed
	 * on the first column of each row.
	 *
	 * N.b.: If multiple rows have the same first column value, the last
	 * row with that value will override earlier rows.
	 *
	 * @param string $query
	 * @param array $values Values to bind to the query.
	 *
	 * @param null $callable
	 * @param string $key_field
	 * @return array
	 * @throws Exception
	 *
	 */
	public function fetchAssoc($query, array $values = [], $callable = null, $key_field = ''){
		$statement = $this->perform($query, $values);

		$data = [];
		while ($row = $statement->fetch(self::FETCH_ASSOC)){
			if ($callable){
				$row = call_user_func($callable, $row);
			}
			$key = !empty($key_field) ? $row[$key_field] : current($row);
			$data[$key] = $row;
		}

		return $data;
	}

	/**
	 *
	 * Fetches one field from one row of the database as a scalar value.
	 *
	 * @param array $args
	 * @return string
	 * @internal param string $statement The SQL statement to prepare and execute.
	 *
	 * @internal param array $values Values to bind to the query.
	 *
	 */
	public function fetchField(...$args){
		$data = $this->fetchOne(...$args);
		$value = is_array($data) ? reset($data) : '';

		return $value;
	}

	/**
	 *
	 * Performs a query and then returns the last inserted ID
	 *
	 * @param string $query The SQL statement to prepare and execute.
	 *
	 * @param array $values Values to bind to the query.
	 *
	 * @return integer
	 *
	 */
	public function performId($query, $values){
		$this->perform($query, $values);

		return $this->lastInsertId();
	}

	/**
	 * @param string $table_name
	 * @param array $vals
	 * @param array $include_keys
	 * @return int
	 */
	public function insert($table_name, array $vals, array $include_keys){
		$query_values = self::makeQueryInsert($vals, $include_keys);
		/** @noinspection SqlResolve */
		$query = "INSERT INTO `$table_name` " . $query_values->query;

		return $this->performId($query, $query_values->values);
	}

	/**
	 * @param $table_name
	 * @param array $values
	 * @param array $include_keys
	 * @return int
	 *
	 * @deprecated use insert()
	 */
	public function queryInsert($table_name, array $values, array $include_keys){
		return $this->insert($table_name, $values, $include_keys);
	}

	/**
	 * @param string $table_name
	 * @param array $values
	 * @param array $exclude_keys
	 * @return int
	 */
	public function insertExc($table_name, array $values, array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->insert($table_name, $values, $include_keys);
	}

	/**
	 * @param $table_name
	 * @param array $values
	 * @param array $exclude_keys
	 * @return int
	 *
	 * @deprecated use insertExc()
	 */
	public function queryInsertExc($table_name, array $values, array $exclude_keys = []){
		return $this->insertExc($table_name, $values, $exclude_keys);
	}

	/**
	 * @param string $table_name
	 * @param string|array $where
	 * @param array $values
	 * @param array $include_keys
	 * @return int
	 */
	public function update($table_name, $where, array $values, array $include_keys){
		$query_values = self::makeQueryUpdate($table_name, $where, $values, $include_keys);

		return $this->fetchAffected($query_values->query, $query_values->values);
	}

	/**
	 * @param $table_name
	 * @param $where
	 * @param array $values
	 * @param array $include_keys
	 * @return int
	 *
	 * @deprecated use update()
	 */
	public function queryUpdate($table_name, $where, array $values, array $include_keys){
		return $this->update($table_name, $where, $values, $include_keys);
	}

	/**
	 * @param $table_name
	 * @param $where
	 * @param array $vals
	 * @param array $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryUpdate($table_name, $where, array $vals, array $include_keys){
		$query_values = self::makeQueryUpdateComponents($vals, $include_keys);
		$values = $query_values->values;

		$where_query = self::whereQuery($where, $values);

		if (is_array($where)){
			$where = self::makeQueryUpdateWhere($where, $values);
			$values = array_merge($values, $where);
		}
		$table_name = "`$table_name`";
		$query = "UPDATE $table_name SET {$query_values->query} WHERE $where_query";

		return new ExtendedPdoQueryValues($query, $values);
	}

	/**
	 * @param array $where
	 * @param array $values
	 * @return array
	 */
	public static function makeQueryUpdateWhere(array $where, array $values){
		$where_copy = [];
		foreach ($where as $key => $val){
			if (isset($values[$key])){
				$key = $key . self::$where_key_collision;
			}
			$where_copy[$key] = $val;
		}

		return $where_copy;
	}

	/**
	 * @param array|string $where
	 * @param array $values
	 * @return string
	 */
	public static function whereQuery($where, $values = []){
		if (is_array($where)){
			$where_keys = [];
			foreach ($where as $key => $val){
				$operator = '=';
				$param_key = isset($values[$key]) ? ($key . self::$where_key_collision) : $key;
				$where_keys[] = "`$key` $operator :$param_key";
			}
			$where_query = implode(' and ', $where_keys);
		}
		else {
			$where_query = $where;
		}

		return $where_query;
	}

	/**
	 * @param array $where
	 * @param array $operators
	 * @return string
	 */
	public static function whereQueryOperator(array $where, array $operators){
		$where_keys = [];
		foreach ($where as $key => $val){
			$operator = '=';
			if (!empty($operators[$key])){
				$operator = $operators[$key];
			}
			$where_keys[] = "`$key` $operator :$key";
		}
		$where_query = implode(' and ', $where_keys);

		return $where_query;
	}

	/**
	 * @param string $table_name
	 * @param array|string $where
	 * @param array $values
	 * @param array $exclude_keys
	 * @return int
	 */
	public function updateExc($table_name, $where, array $values, array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->update($table_name, $where, $values, $include_keys);
	}

	/**
	 * @param $table_name
	 * @param $where
	 * @param array $values
	 * @param array $exclude_keys
	 * @return int
	 *
	 * @deprecated use updateExc()
	 */
	public function queryUpdateExc($table_name, $where, array $values, array $exclude_keys = []){
		return $this->updateExc($table_name, $where, $values, $exclude_keys);
	}

	/**
	 * @param array $values
	 * @param array $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryInsert(array $values, array $include_keys){
		$query_placeholders = self::makeQueryValues('insert', $values, $include_keys);
		$query = "(" . implode(', ', $query_placeholders->fields) . ") VALUES (" . implode(',', $query_placeholders->placeholders) . ")";

		return new ExtendedPdoQueryValues($query, $query_placeholders->vals);
	}

	/**
	 * @param array $values
	 * @param array $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryUpdateComponents(array $values, array $include_keys){
		$query_placeholders = self::makeQueryValues('update', $values, $include_keys);
		$query = implode(', ', $query_placeholders->fields);

		return new ExtendedPdoQueryValues($query, $query_placeholders->vals);
	}

	/**
	 * @param $type
	 * @param array $values
	 * @param array $include_keys
	 * @return ExtendedPdoQueryPlaceholders
	 */
	public static function makeQueryValues($type, array $values, array $include_keys){
		$fields = $placeholders = $vals = [];
		$values_keys = array_keys($values);

		foreach ($include_keys as $key){
			$key = trim($key);
			$val = $values[$key];
			$key_is_set = in_array($key, $values_keys);
			$value_isnt_false = $val!==false;
			$key_has_no_dash = strpos($key, '-')===false;
			$value_isnt_array = !is_array($val);

			if ($value_isnt_false && $key_has_no_dash && $value_isnt_array && $key_is_set){
				$placeholder = ":$key";
				$fields[] = $type==='update' ? "`$key` = $placeholder" : "`$key`";
				$placeholders[] = $placeholder;
				$vals[$key] = !is_null($val) ? $val : '';
			}
		}

		return new ExtendedPdoQueryPlaceholders($fields, $placeholders, $vals);
	}

	/**
	 * @param string $statement
	 * @param array $values
	 * @return \PDOStatement
	 * @throws Exception
	 */
	public function perform($statement, array $values = []){
		$sth = $this->prepareWithValues($statement, $values);
		$this->beginProfile(__FUNCTION__);
		try {
			$sth->execute();
		}
		catch (PDOException $e) {
			throw new Exception($e->getMessage(), $sth->queryString);
		}
		$this->endProfile($statement, $values);

		return $sth;
	}

	// We actually might not need this, as ExtendedPdo can fill
	//  in arrays for us as part of its "prepare" process
	public static function prepareList(array $arr = []){
		$count = count($arr);
		$list = [];
		for ($n = 0; $n<$count; $n++){
			$list[] = '?';
		}

		return implode(',', $list);
	}

	/**
	 * We don't typehint the $where value because it can be a manually typed
	 * string for things such as > < between etc.
	 * @param string $table
	 * @param array|string $where
	 * @param string $type
	 * @param string $fields
	 * @return array|string
	 */
	public function selectFrom($table, $where, $type = 'one', $fields = "*"){
		$query = self::selectFromQuery($table, $where, $fields);
		$func = 'fetch' . ucfirst($type);
		// if we had a string $where, pass on an empty array as where
		if (!is_array($where)){
			$where = [];
		}
		$data = $this->$func($query, $where);

		return $data;
	}

	/**
	 * @param string $table
	 * @param array|string $where
	 * @param array|string $fields
	 * @return string
	 */
	public static function selectFromQuery($table, $where, $fields){
		$where_query = self::whereQuery($where);

		$fields = self::fieldFormat($fields);

		/** @noinspection SqlResolve */
		$query = "SELECT $fields FROM `$table` WHERE $where_query";

		return $query;
	}

	/**
	 * @param array|string $fields
	 * @return string
	 */
	private static function fieldFormat($fields){
		if (!is_array($fields)){
			$fields = explode(',', $fields);
		}
		foreach ($fields as &$field){
			$field = trim($field);
			if (strpos($field, '*')===false){
				$field = "`$field`";
			}
		}
		$fields = implode(',', $fields);

		return $fields;
	}

	/**
	 * @param string $table
	 * @param array|string $where
	 * @return int
	 */
	public function selectCount($table, $where){
		return $this->selectFrom($table, $where, 'field', ['count(*)']);
	}

	/**
	 * @param string $table
	 * @param array|string $where
	 * @param string $fields
	 * @return array
	 */
	public function selectOne($table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'one', $fields);
	}

	/**
	 * @param string $table
	 * @param array|string $where
	 * @param string $fields
	 * @return array
	 */
	public function selectAll($table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'all', $fields);
	}

	/**
	 * @param string $table
	 * @param array|string $where
	 * @param string $field
	 * @return string
	 */
	public function selectField($table, $where, $field){
		return $this->selectFrom($table, $where, 'field', $field);
	}

	/**
	 * @param string $table
	 * @param array|string $where
	 * @param null $limit
	 * @return int
	 * @throws Exception
	 */
	public function delete($table, $where, $limit = null){
		$query_values = self::deleteQuery($table, $where, $limit);

		return $this->fetchAffected($query_values->query, $query_values->values);
	}

	/**
	 * @param $table
	 * @param $where
	 * @param $limit
	 * @return ExtendedPdoQueryValues
	 * @throws Exception
	 */
	public static function deleteQuery($table, $where, $limit){
		$where_query = self::whereQuery($where);
		/** @noinspection SqlResolve */
		$query = "DELETE FROM `$table` WHERE $where_query";
		if (empty($where_query)){
			throw new Exception('Delete commands must contain a WHERE component.', $query);
		}
		if (!empty($limit)){
			$query .= " LIMIT :_limit";
			$where['_limit'] = $limit;
		}

		return new ExtendedPdoQueryValues($query, $where);
	}
}

/**
 * Class ExtendedPdoQueryPlaceholders
 * @package M1ke\Sql
 *
 * Acts as a hintable return type for makeQueryValues
 */
class ExtendedPdoQueryPlaceholders {
	/**
	 * @var array
	 */
	public $fields;
	/**
	 * @var array
	 */
	public $placeholders;
	/**
	 * @var array
	 */
	public $vals;

	public function __construct(array $fields, array $placeholders, array $vals){
		$this->fields = $fields;
		$this->placeholders = $placeholders;
		$this->vals = $vals;
	}
}

/**
 * Class ExtendedPdoQueryValues
 * @package M1ke\Sql
 *
 * Acts as a hintable return type for query/value mixed generation
 */
class ExtendedPdoQueryValues {
	/**
	 * @var string
	 */
	public $query;
	/**
	 * @var array
	 */
	public $values = [];

	/**
	 * ExtendedPdoQueryValues constructor.
	 * @param string $query
	 * @param array $values
	 */
	public function __construct($query, array $values = []){

		$this->query = $query;
		$this->values = $values;
	}
}
