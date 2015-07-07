<?php
namespace M1ke\Sql;

use Aura\Sql\Exception as AuraException;

class Exception extends AuraException {
	private $query;

	public function __construct($message, $query){
		$this->message = $message;
		$this->query = $query;
	}

	public function getQuery(){
		return $this->query;
	}
}