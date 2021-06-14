<?php
namespace M1ke\Sql;

use Aura\Sql\ExtendedPdo as AuraPdo;

use PDOException;

/**
 *
 * Expands Aura's ExtendedPdo to add database manipulation helpers
 *
 * @package M1ke.Sql
 *
 */
class ExtendedPdo extends AuraPdo {
	/** @internal */
	public const KEY_COLLISION = '____';

	/**
	 * ExtendedPdo constructor.
	 * @param string $db
	 * @param string $user
	 * @param string $pass
	 * @param string $charset
	 * @param string $type
	 * @param string $server
	 */
	public function __construct($db, $user, $pass, array $options = [], $charset = 'utf8', $type = 'mysql', $server = 'localhost'){
		$dsn = "$type:host={$server};dbname={$db}";

		if (!empty($charset)){
			$dsn .= ";charset={$charset}";
		}

		parent::__construct($dsn, $user, $pass, $options);
	}

	/**
	 * @psalm-param array<string, scalar> $values
	 * @psalm-param list<string> $exclude_keys
	 * @return array
	 * @psalm-return list<string>
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
	 * @psalm-param array<string, ?scalar|list<scalar>> $values
	 * @param ?callable $callable
	 * @return array
	 */
	protected function fetchAllWithCallable($fetch_type, $statement, array $values = [], $callable = null){
		$args = func_get_args();
		$return = parent::fetchAllWithCallable(...$args);

		return is_array($return) ? $return : [];
	}

	/**
	 * @param string $statement
	 * @psalm-param array<string, ?scalar|list<scalar>> $values
	 * @return array
	 * @psalm-return array|empty
	 */
	public function fetchOne($statement, array $values = []){
		$return = parent::fetchOne($statement, $values);

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
	 * @param string $statement
	 * @psalm-param array<string, ?scalar|list<scalar>> $values
	 *
	 * @param ?callable $callable
	 * @param ?string $key_field
	 *
	 * @return array
	 * @psalm-return array<string, array>
	 *
	 * @throws Exception
	 */
	public function fetchAssoc($statement, array $values = [], $callable = null, $key_field = ''){
		$stmt = $this->perform($statement, $values);

		$data = [];
		while ($row = $stmt->fetch(self::FETCH_ASSOC)){
			if ($callable){
				$row = call_user_func($callable, $row);
			}
			$key = !empty($key_field) ? $row[$key_field] : current($row);
			$data[$key] = $row;
		}

		return $data;
	}

	/**
	 * Fetches one field from one row of the database as a scalar value.
	 *
	 * @param string $statement
	 * @psalm-param array<string, ?scalar|list<scalar>> $values
	 *
	 * @return string
	 */
	public function fetchField($statement, array $values = []){
		$data = $this->fetchOne($statement, $values);

		return self::fetchFieldReturn($data);
	}

	/**
	 * @param array $data
	 * @return mixed|string
	 */
	public static function fetchFieldReturn($data){
		return (is_array($data) && !empty($data)) ? reset($data) : '';
	}

	/**
	 *
	 * Performs a query and then returns the last inserted ID
	 *
	 * @param string $query The SQL statement to prepare and execute.
	 * @psalm-param array<string, ?scalar> $values
	 *
	 * @return int
	 *
	 * @throws Exception
	 */
	public function performId($query, array $values = []){
		$this->perform($query, $values);

		return $this->lastInsertId();
	}

	/**
	 * @param string $table_name
	 * @psalm-param array<string, scalar> $values
	 * @psalm-param list<string> $include_keys
	 * @return int
	 * @throws Exception
	 */
	public function insert($table_name, array $vals, array $include_keys){
		$query_values = self::makeQueryInsert($vals, $include_keys);
		/** @noinspection SqlResolve */
		$query = "INSERT INTO `$table_name` " . $query_values->query;

		return $this->performId($query, $query_values->values);
	}

	/**
	 * @param string $table_name
	 * @psalm-param array<string, scalar> $values
	 * @psalm-param list<string> $exclude_keys
	 * @return int
	 * @throws Exception
	 */
	public function insertExc($table_name, array $values, array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->insert($table_name, $values, $include_keys);
	}

	/**
	 * @param string $table_name
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @psalm-param array<string, scalar> $vals
	 * @psalm-param list<string> $include_keys
	 * @return int
	 */
	public function update($table_name, $where, array $values, array $include_keys){
		$query_values = self::makeQueryUpdate($table_name, $where, $values, $include_keys);

		return $this->fetchAffected($query_values->query, $query_values->values);
	}

	/**
	 * @param string $table_name
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @psalm-param array<string, scalar> $vals
	 * @psalm-param list<string> $include_keys
	 * @return int
	 */
	public function updateOne($table_name, $where, array $values, array $include_keys){
		$query_values = self::makeQueryUpdate($table_name, $where, $values, $include_keys);

		$query = $query_values->query;
		$query .= " LIMIT 1";

		return $this->fetchAffected($query, $query_values->values);
	}

	/**
	 * @param string $table_name
	 * @param array|string $where
	 * @psalm-param array<string, scalar> $values
	 * @psalm-param list<string> $exclude_keys
	 * @return int
	 */
	public function updateExc($table_name, $where, array $values, array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->update($table_name, $where, $values, $include_keys);
	}

	/**
	 * @param string $table_name
	 * @param array|string $where
	 * @psalm-param array<string, scalar> $values
	 * @psalm-param list<string> $exclude_keys
	 * @return int
	 */
	public function updateOneExc($table_name, $where, array $values, array $exclude_keys = []){
		$include_keys = self::excludeKeys($values, $exclude_keys);

		return $this->updateOne($table_name, $where, $values, $include_keys);
	}

	/**
	 * @param string $table_name
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @psalm-param array<string, scalar> $vals
	 * @psalm-param list<string> $include_keys
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
	 * @psalm-param array<string, scalar> $where
	 * @psalm-param array<string, ?scalar> $values
	 * @return array
	 * @psalm-return array<string, ?scalar>
	 */
	public static function makeQueryUpdateWhere(array $where, array $values){
		$where_copy = [];
		foreach ($where as $key => $val){
			if (isset($values[$key])){
				$key = $key . self::KEY_COLLISION;
			}
			if ($val===false){
				$val = null;
			}
			$where_copy[$key] = $val;
		}

		return $where_copy;
	}

	/**
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param array $values
	 * @psalm-param array<string, ?scalar> $values
	 * @return string
	 */
	public static function whereQuery($where, $values = []){
		if (is_array($where)){
			$where_keys = [];
			foreach ($where as $key => $val){
				$operator = '=';
				$param_key = isset($values[$key]) ? ($key . self::KEY_COLLISION) : $key;
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
	 * @return string
	 */
	public static function whereQueryOperator(array $where, array $operators){
		$where_keys = [];
		foreach ($where as $key => $val){
			$operator = '=';
			if (!empty($operators[$key])){
				$operator = $operators[$key];
			}

			[$key, $pfx] = self::splitPrefix($key);

			$where_keys[] = "$pfx`$key` $operator :$key";
		}

		return implode(' and ', $where_keys);
	}

	/**
	 * @param string $key
	 * @psalm-return array{non-empty-string, string}
	 */
	protected static function splitPrefix($key): array{
		$parts = explode('.', $key);
		if (count($parts)>1){
			$pfx = $parts[0].'.';
			$key = $parts[1];
		}
		else {
			$pfx = '';
			$key = $parts[0];
		}
		return [$key, $pfx];
	}

	/**
	 * @psalm-param array<string, scalar> $values
	 * @psalm-param list<string> $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryInsert(array $values, array $include_keys){
		$query_placeholders = self::makeQueryValues('insert', $values, $include_keys);
		$query = "(" . implode(', ', $query_placeholders->fields) . ") VALUES (" . implode(',', $query_placeholders->placeholders) . ")";

		return new ExtendedPdoQueryValues($query, $query_placeholders->vals);
	}

	/**
	 * @psalm-param array<string, ?scalar> $values
	 * @psalm-param list<string> $include_keys
	 * @return ExtendedPdoQueryValues
	 */
	public static function makeQueryUpdateComponents(array $values, array $include_keys){
		$query_placeholders = self::makeQueryValues('update', $values, $include_keys);
		$query = implode(', ', $query_placeholders->fields);

		return new ExtendedPdoQueryValues($query, $query_placeholders->vals);
	}

	/**
	 * @param string $type
	 * @psalm-type 'insert'|'update' $type
	 * @psalm-param array<string, ?scalar> $values
	 * @psalm-param list<string> $include_keys
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
	 * @psalm-param array<string, ?scalar> $values
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
	public static function prepareList(array $arr = []): string{
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
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param string $type
	 * @psalm-type 'one'|'assoc'|'all'|'field' $type
	 * @param string|array $fields
	 * @psalm-param string|list<string> $fields
	 * @return array|string
	 */
	public function selectFrom($table, $where, $type = 'one', $fields = "*"){
		$query = self::selectFromQuery($table, $where, $fields);
		$func = 'fetch' . ucfirst($type);
		// if we had a string $where, pass on an empty array as where
		if (!is_array($where)){
			$where = [];
		}
		else {
			$where = $this->removeFalseWhere($where);
		}

		return $this->$func($query, $where);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param string|array $fields
	 * @psalm-param string|list<string> $fields
	 * @return string
	 */
	public static function selectFromQuery($table, $where, $fields){
		$where_query = self::whereQuery($where);

		$fields = self::fieldFormat($fields);

		/** @noinspection SqlResolve */
		return "SELECT $fields FROM `$table`".(!empty($where_query) ? " WHERE $where_query" : '');
	}

	/**
	 * @param string|array $fields
	 * @psalm-param string|list<string> $fields
	 * @return string
	 */
	private static function fieldFormat($fields){
		if (!is_array($fields)){
			$fields = explode(',', $fields);
		}
		foreach ($fields as &$field){
			$field = trim($field);
			if (strpos($field, '*')===false){
				[$key, $pfx] = self::splitPrefix($field);
				$field = "$pfx`$key`";
			}
		}

		return implode(',', $fields);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @return int
	 */
	public function selectCount($table, $where){
		return (int) $this->selectFrom($table, $where, 'field', ['count(*)']);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param string $fields
	 * @return array
	 * @psalm-return array|empty
	 */
	public function selectOne($table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'one', $fields);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param string $fields
	 * @return array
	 * @psalm-return list<array>
	 */
	public function selectAll($table, $where, $fields = "*"){
		return $this->selectFrom($table, $where, 'all', $fields);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param string $field
	 * @return string
	 */
	public function selectField($table, $where, $field){
		return $this->selectFrom($table, $where, 'field', $field);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param ?int $limit
	 * @return int
	 * @throws Exception
	 */
	public function delete($table, $where, $limit = null){
		$query_values = self::deleteQuery($table, $where, $limit);

		return $this->fetchAffected($query_values->query, $query_values->values);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @return int
	 * @throws Exception
	 */
	public function deleteOne($table, $where){
		return $this->delete($table, $where, 1);
	}

	/**
	 * @param string $table
	 * @param string|array $where
	 * @psalm-param string|array<string, scalar> $where
	 * @param int $limit
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
			if (is_array($where)){
				$query .= " LIMIT :_limit";
				$where['_limit'] = $limit;
			}
			else {
				$query .= " LIMIT $limit";
			}
		}

		return new ExtendedPdoQueryValues($query, is_array($where) ? $where : []);
	}

	/**
	 * @template T of array<string, scalar>
	 * @param array $where
	 * @psalm-param T $where
	 * @return array
	 * @psalm-return T
	 */
	private function removeFalseWhere($where){
		foreach ($where as &$val){
			if ($val===false){
				$val = null;
			}
		}

		return $where;
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
	 * @psalm-var list<string>
	 */
	public $fields;
	/**
	 * @var array
	 * @psalm-var list<string>
	 */
	public $placeholders;
	/**
	 * @var array
	 * @psalm-var array<string, ?scalar>
	 */
	public $vals;

	/**
	 * @psalm-param list<string> $fields
	 * @psalm-param list<string> $placeholders
	 * @psalm-param array<string, ?scalar> $vals
	 */
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
	 * @psalm-var array<string, ?scalar>
	 */
	public $values;

	/**
	 * ExtendedPdoQueryValues constructor.
	 * @param string $query
	 * @psalm-param array<string, ?scalar> $values
	 */
	public function __construct($query, array $values = []){
		$this->query = $query;
		$this->values = $values;
	}
}
