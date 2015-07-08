<?php

require __DIR__.'/../src/ExtendedPdo.php';
require __DIR__.'/../vendor/autoload.php';

use M1ke\Sql\ExtendedPdo;

class TestBasic extends PHPUnit_Framework_TestCase {
	public function testClassInstantiates(){
		$pdo = new ExtendedPdo($db, $user, $pass);
		$this->assertTrue($pdo instanceof \Aura\Sql\ExtendedPdo);
		$this->assertTrue($pdo instanceof \PDO);
	}
}