<?php

use \Core3\Sql;

/**
 * @group Database
 */
class PdoDriverSqliteTest extends \PHPUnit_Framework_TestCase
{
    protected $db;
    protected $dbFile;

    function setUp()
    {
        $this->dbFile = tempnam("/tmp", "sqlite");

        $this->db = new \Core3\Sql\PdoDriver('sqlite:'.$this->dbFile);

        $this->db->query(
            'CREATE TABLE CoreUser ('.
            'id INTEGER PRIMARY KEY,'.      // sqlite style for auto increment
            'username VARCHAR(50),'.
            'password VARCHAR(255)'.        // password_hash() currently uses 60-byte strings
            ')'
        );
    }

    function tearDown()
    {
        unlink($this->dbFile);
    }

    function testSqlite()
    {
        $this->assertEquals(true, $this->db->isConnected());

        $id = $this->db->insert(
            'INSERT INTO CoreUser (username, password) VALUES (:user, :pass)',
            array(':user' => 'kalle', ':pass' => 'pwd')
        );
        $this->assertEquals(1, $id);

        $id = $this->db->insert(
            'INSERT INTO CoreUser (username, password) VALUES (:user, :pass)',
            array(':user' => 'nisse', ':pass' => 'pwd2')
        );
        $this->assertEquals(2, $id);


        $res = $this->db->select('SELECT * FROM CoreUser');
        $this->assertInternalType('array', $res);
        $this->assertEquals(2, count($res));


        $res = $this->db->selectRow('SELECT * FROM CoreUser WHERE id = :id', array(':id' => 1));
        $this->assertEquals(array('id'=>1,'username'=>'kalle','password'=>'pwd'), $res);
    }
}
