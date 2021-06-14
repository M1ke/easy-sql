<?php

require __DIR__.'/_bootstrap.php';

use M1ke\Sql\Exception as ExtendedPdoException;
use M1ke\Sql\ExtendedPdo;
use PHPUnit\Framework\TestCase;

class TestExtendedPdo extends TestCase {
	public function testClassInstantiates(){
		$pdo = new ExtendedPdo('', '', '');
		self::assertInstanceOf(\Aura\Sql\ExtendedPdo::class, $pdo);
		self::assertInstanceOf(PDO::class, $pdo);
	}

	public function testMakeQueryInsert(){
		$vals = [
			'string'=> 'test',
			'not'=> 'nope',
		];
		$include_keys = ['string'];

		$query_values = ExtendedPdo::makeQueryInsert($vals, $include_keys);

		self::assertEquals("(`string`) VALUES (:string)", $query_values->query);
		self::assertEquals([
			'string'=> 'test',
		], $query_values->values);
	}

	public function testMakeQueryUpdate(){
		$table_name = 'table';
		$where = ['key'=> 1];
		$vals = [
			'string'=> 'test',
			'not'=> 'nope',
		];
		$include_keys = ['string'];

		$query_values = ExtendedPdo::makeQueryUpdate($table_name, $where, $vals, $include_keys);

		/** @noinspection SqlResolve */
		self::assertEquals("UPDATE `table` SET `string` = :string WHERE `key` = :key", $query_values->query);
		self::assertEquals([
			'string'=> 'test',
			'key'=> 1,
		], $query_values->values);
	}

	public function testMakeQueryUpdateWhereString(){
		$table_name = 'table';
		$where = '`key`=1';
		$vals = [
			'string'=> 'test',
			'not'=> 'nope',
		];
		$include_keys = ['string'];

		$query_values = ExtendedPdo::makeQueryUpdate($table_name, $where, $vals, $include_keys);

		/** @noinspection SqlResolve */
		self::assertEquals("UPDATE `table` SET `string` = :string WHERE `key`=1", $query_values->query);
		self::assertEquals([
			'string'=> 'test',
		], $query_values->values);
	}

	public function testMakeQueryUpdateWhere(){
		$value = 'val';
		$key = 'test';
		$where = [
			$key => $value,
		];
		$values = $where;
		$where['falsey'] = false;

		$where_new = ExtendedPdo::makeQueryUpdateWhere($where, $values);

		$collision_string = ExtendedPdo::KEY_COLLISION;
		self::assertSame([$key.$collision_string=> $value, 'falsey' => null], $where_new);
	}

	public function testMakeQueryUpdateRepeatedField(){
		$table_name = 'table';
		$where = [
			'key'=> 1,
			'string'=>'old string'
		];
		$vals = [
			'string'=> 'test',
		];
		$include_keys = ['string'];
		$collision_string = ExtendedPdo::KEY_COLLISION;

		$query_values = ExtendedPdo::makeQueryUpdate($table_name, $where, $vals, $include_keys);

		/** @noinspection SqlResolve */
		self::assertEquals("UPDATE `table` SET `string` = :string WHERE `key` = :key and `string` = :string$collision_string", $query_values->query);
		self::assertEquals([
			'string'=> 'test',
			'key'=> 1,
			'string'.$collision_string=> 'old string',
		], $query_values->values);
	}

	public function testWhereQuery(){
		$where = [
			'id'=> 1,
			'string'=> 'test',
		];
		$where_query = ExtendedPdo::whereQuery($where);
		self::assertEquals("`id` = :id and `string` = :string", $where_query);
	}

	public function testWhereQueryString(){
		$where = "id = :id and string = :string";
		$where_query = ExtendedPdo::whereQuery($where);
		self::assertEquals("id = :id and string = :string", $where_query);
	}

	public function testWhereQueryOperator(){
		$where = [
			'id'=> 1,
			'date'=> '2015-07-30',
			'a.number'=> 6,
			'bad'=> 'test'
		];
		$where_query = ExtendedPdo::whereQueryOperator($where, ['date'=> '>=', 'a.number'=> '<', 'unset'=> '<>' , 'bad'=> '']);
		self::assertEquals("`id` = :id and `date` >= :date and a.`number` < :number and `bad` = :bad", $where_query);
	}

	public function testMakeQueryValuesInsert(){
		$values = [
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'false'=> false,
			'null'=> null,
			'ignored'=> 'rawr',
		];
		$include_keys = ['id', 'string', 'empty', 'false', 'null', 'doesnt_exist'];

		$query_placeholders = ExtendedPdo::makeQueryValues('insert', $values, $include_keys);

		self::assertEquals(['`id`', '`string`', '`empty`', '`null`'], $query_placeholders->fields);
		self::assertEquals([':id', ':string', ':empty', ':null'], $query_placeholders->placeholders);
		self::assertEquals([
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'null'=> '',
		], $query_placeholders->vals);
	}

	public function testMakeQueryValuesUpdate(){
		$values = [
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'false'=> false,
			'null'=> null,
			'ignored'=> 'rawr',
		];
		$include_keys = ['id', 'string', 'empty', 'false', 'null'];

		$query_placeholders = ExtendedPdo::makeQueryValues('update', $values, $include_keys);

		self::assertEquals(['`id` = :id', '`string` = :string', '`empty` = :empty', '`null` = :null'], $query_placeholders->fields);
		// Placeholders should be the same as "testMakeQueryValuesInsert" AND are irrelevant for update
		// Vals should be the same as "testMakeQueryValuesInsert"
		self::assertEquals([
			'id'=> 1,
			'string'=> 'test',
			'empty'=> '',
			'null'=> '',
		], $query_placeholders->vals);
	}

	public function testSelectFromQuery(){
		$table = 'test';
		$where = [
			'id' => 1,
		];
		$fields = '*';
		$query = ExtendedPdo::selectFromQuery($table, $where, $fields);
		/** @noinspection SqlResolve */
		self::assertEquals("SELECT * FROM `test` WHERE `id` = :id", $query);
	}

	public function testSelectFromQueryFieldArray(){
		$table = 'test';
		$where = [
			'id' => 1,
		];
		$fields = ['id', 'string'];
		$query = ExtendedPdo::selectFromQuery($table, $where, $fields);
		/** @noinspection SqlResolve */
		self::assertEquals("SELECT `id`,`string` FROM `test` WHERE `id` = :id", $query);
	}

	public function testSelectFromQueryWhereString(){
		$table = 'test';
		$where = "id = 1";
		$fields = 't.string, id';
		$query = ExtendedPdo::selectFromQuery($table, $where, $fields);
		/** @noinspection SqlResolve */
		self::assertEquals("SELECT t.`string`,`id` FROM `test` WHERE id = 1", $query);
	}

	public function testExcludeKeys(){
		$values = [
			'id'=> 1,
			'name'=> 'Test',
			'excluded'=> 'string',
		];
		$exclude_keys = ['excluded'];
		$include_keys = ExtendedPdo::excludeKeys($values, $exclude_keys);
		self::assertEquals(['id', 'name'], $include_keys);
	}

	public function testExcludeKeysIncludeAll(){
		$values = [
			'id'=> 1,
			'name'=> 'Test',
			'excluded'=> 'string',
		];
		$exclude_keys = [];
		$include_keys = ExtendedPdo::excludeKeys($values, $exclude_keys);
		self::assertEquals(['id', 'name', 'excluded'], $include_keys);
	}

	public function testDsnDefault(){
		$extended_pdo = new ExtendedPdo('test', 'user', 'pass');
		$dsn = $extended_pdo->getDsn();
		self::assertEquals('mysql:host=localhost;dbname=test;charset=utf8', $dsn);
	}

	public function testDsnCustom(){
		$extended_pdo = new ExtendedPdo('test', 'user', 'pass', [], 'latin1', 'sqlite', 'server.com');
		$dsn = $extended_pdo->getDsn();
		self::assertEquals('sqlite:host=server.com;dbname=test;charset=latin1', $dsn);
	}

	public function testDeleteQuery(){
		$id = 1;
		$query_values = ExtendedPdo::deleteQuery('table', ['id'=> $id], 0);
		/** @noinspection SqlResolve */
		self::assertEquals("DELETE FROM `table` WHERE `id` = :id", $query_values->query);
		self::assertEquals(['id'=> $id], $query_values->values);
	}

	public function testDeleteQueryWithLimit(){
		$id = 1;
		$limit = 2;
		$query_values = ExtendedPdo::deleteQuery('table', ['id'=> $id], $limit);
		/** @noinspection SqlResolve */
		self::assertEquals("DELETE FROM `table` WHERE `id` = :id LIMIT :_limit", $query_values->query);
		self::assertEquals(['id'=> $id, '_limit'=> $limit], $query_values->values);
	}

	public function testDeleteQueryStringWithLimit(){
		$id = 1;
		$limit = 2;
		$query_values = ExtendedPdo::deleteQuery('table', "`status` <> 0", $limit);
		/** @noinspection SqlResolve */
		self::assertEquals("DELETE FROM `table` WHERE `status` <> 0 LIMIT $limit", $query_values->query);
		self::assertEquals([], $query_values->values);
	}

	public function testDeleteQueryNoWhere(){
		try {
			ExtendedPdo::deleteQuery('table', [], 0);
			self::assertTrue(false, 'The ExtendedPdoException was not thrown');
		}
		catch (ExtendedPdoException $e){
			self::assertTrue(true);
		}
	}

	public function testFetchFieldReturnArray(){
		$val = ExtendedPdo::fetchFieldReturn(['test' => 'a']);

		self::assertSame('a', $val);
	}

	public function testFetchFieldReturn(){
		$val = ExtendedPdo::fetchFieldReturn(null);

		self::assertSame('', $val);
	}

	public function testFetchFieldReturnArrayEmpty(){
		$val = ExtendedPdo::fetchFieldReturn([]);

		self::assertSame('', $val);
	}
}
