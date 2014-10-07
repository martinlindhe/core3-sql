<?php
namespace Core3\Sql;

/**
 * MySQL driver using the PDO extension
 */
class PdoDriver
{
    protected $driver;
    protected $dbHandle = null;
    protected $server;
    protected $port;
    protected $username;
    protected $password;
    protected $database;
    protected $charset;

    public function __construct($driver)
    {
        $this->driver = $driver;
    }

    public function setServer($s)
    {
        $this->server = $s;
    }

    public function setPort($n)
    {
        $this->port = $n;
    }

    public function setUsername($s)
    {
        $this->username = $s;
    }

    public function setPassword($s)
    {
        $this->password = $s;
    }

    public function setDatabase($s)
    {
        $this->database = $s;
    }

    public function getServer()
    {
        return $this->server;
    }

    public function isConnected()
    {
        return $this->dbHandle !== null;
    }

    private function getPdoConnection($config)
    {
        $param = '';
        if (!empty($config)) {
            $param = ':'.implode(';', $config);
        }

        $dsn = $this->driver.$param;

        try {
            $pdo = new \PDO($dsn, $this->username, $this->password);
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        } catch (\PDOException $e) {
            throw new \Core3\Sql\Exception\ConnectionFailed();
        }

        return $pdo;
    }

    public function connect()
    {
        if ($this->dbHandle !== null) {
            throw new \Core3\Sql\Exception\AlreadyConnected();
        }

        $config = array();

        if ($this->driver == 'mysql') {
            if (!$this->port) {
                $this->port = 3306;
            }

            if (!$this->charset) {
                $this->charset = 'utf8';
            }
        }

        if ($this->server) {
            $config[] = 'host='.$this->server;
        }

        if ($this->port) {
            $config[] = 'port='.$this->port;
        }

        if ($this->database) {
            $config[] = 'dbname='.$this->database;
        }

        if ($this->charset) {
            $config[] = 'charset='.$this->charset;
        }

        $this->dbHandle = $this->getPdoConnection($config);
    }

    public function disconnect()
    {
        if ($this->dbHandle !== null) {
            $this->dbHandle = null;
        }
    }

    /**
     * Prepares a statement with named variables (:name)
     * @param $args[0] string  sql query
     * @param $args[1] array (name => value keys)
     */
    private function execute($args)
    {
        if (!$args[0]) {
            throw new \Core3\Sql\Exception\InvalidArgument();
        }

        if ($this->dbHandle === null) {
            $this->connect();
        }

        $stmt = $this->dbHandle->prepare($args[0]);

        if (!isset($args[1])) {
            $args[1] = array();
        }

        try {
            $res = $stmt->execute($args[1]);
        } catch (\PDOException $e) {
            throw new \Core3\Sql\Exception\InvalidQuery($e->getMessage());
        }

        return $stmt;
    }

    public function select()
    {
        $stmt = $this->execute(func_get_args());
        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        return $res;
    }

    public function selectToObject()
    {
        $res = $this->selectToObjects(func_get_args());

        return $res[0];
    }

    /**
     * @param $args[0] query
     * @param $args[1..n] parameters
     */
    public function selectToObjects()
    {
        $args = func_get_args();

        if (is_array($args[0])) {
            $args = $args[0]; // XXX hack to satisfy func_get_args()
        }

        $classname = array_shift($args);

        $stmt = $this->execute($args);

        return $stmt->fetchAll(\PDO::FETCH_CLASS|\PDO::FETCH_PROPS_LATE, $classname);
    }

    /**
     * Runs a generic stored procedure
     */
    public function storedProc()
    {
        $stmt = $this->execute(func_get_args());

        return $stmt->fetchAll(\PDO::FETCH_ASSOC);
    }

    public function selectRow()
    {
        $stmt = $this->execute(func_get_args());

        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        if (count($res) > 1) {
            throw new \Core3\Sql\Exception\InvalidResult('returned '.count($res).' rows');
        }

        if (!$res) {
            throw new \Core3\Sql\Exception\InvalidResult();
        }

        return $res[0];
    }

    public function selectItem()
    {
        $stmt = $this->execute(func_get_args());

        if ($stmt->columnCount() != 1) {
            throw new \Core3\Sql\Exception\InvalidResult('expected 1 column, got '.$stmt->columnCount().' columns');
        }

        $res = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        if (count($res) != 1 || count($res[0]) != 1) {
            throw new \Core3\Sql\Exception\InvalidResult();
        }

        return array_shift($res[0]);
    }

    /**
     * Select into 1d array
     */
    public function select1d()
    {
        $stmt = $this->execute(func_get_args());

        $data = array();

        if ($stmt->columnCount() != 1) {
            throw new \Core3\Sql\Exception\InvalidResult('not 1d');
        }

        $res = $stmt->fetchAll(\PDO::FETCH_COLUMN);

        return $res;
    }

    /**
     * Select into mapped array (2 column result set into a key->val array)
     */
    public function selectMapped()
    {
        $stmt = $this->execute(func_get_args());

        $data = array();

        if ($stmt->columnCount() != 2) {
            throw new \Core3\Sql\Exception\InvalidResult('not mapped');
        }

        $fetched = $stmt->fetchAll(\PDO::FETCH_NUM);

        $res = array();
        foreach ( $fetched as $row) {
            $res[ $row[0] ] = $row[1];
        }

        return $res;
    }

    /**
     * @return insert id
     */
    public function insert()
    {
        $stmt = $this->execute(func_get_args());
        return $this->dbHandle->lastInsertId();
    }

    /**
     * @return number of affected rows
     */
    public function delete()
    {
        $stmt = $this->execute(func_get_args());
        return $stmt->rowCount();
    }

    /**
     * @return number of affected rows
     */
    public function update()
    {
        $stmt = $this->execute(func_get_args());
        return $stmt->rowCount();
    }

    /**
     * Run a generic query
     */
    public function query($q)
    {
        if ($this->dbHandle === null) {
            $this->connect();
        }
        $this->dbHandle->query($q);
    }
}
