<?php
namespace Packaged\BigQuery\Tests;

use Google\Cloud\BigQuery\BigQueryClient;
use Packaged\BigQuery\BigQueryException;
use Packaged\BigQuery\BigQueryHelper;
use Packaged\BigQuery\BigQueryType;
use Packaged\BigQuery\IBigQueryWriteable;
use PHPUnit\Framework\TestCase;

class BigQueryHelperTest extends TestCase
{
  const CREDENTIALS = ['type' => 'service_account', 'project_id' => 'test-project', 'client_email' => 'a@b.c'];

  private function _helper($dataSet = 'test_ds'): BigQueryHelper
  {
    return new BigQueryHelper(self::CREDENTIALS, $dataSet);
  }

  /**
   * Calls a private or protected method on the helper
   */
  private function _call(BigQueryHelper $helper, string $method, ...$args)
  {
    $reflection = new \ReflectionMethod($helper, $method);
    if(PHP_VERSION_ID < 80100)
    {
      $reflection->setAccessible(true);
    }
    return $reflection->invoke($helper, ...$args);
  }

  private function _writeable(string $table, string $insertId, array $data, array $schema, $rowTimestamp = null)
  {
    return new TestWriteable($table, $insertId, $data, $schema, $rowTimestamp);
  }

  public function testCredentialsFromArrayJsonAndFile()
  {
    $this->assertSame('test-project', (new BigQueryHelper(self::CREDENTIALS))->gcpProjectName());
    $this->assertSame('test-project', (new BigQueryHelper(json_encode(self::CREDENTIALS)))->gcpProjectName());

    $file = tempnam(sys_get_temp_dir(), 'bq');
    file_put_contents($file, json_encode(self::CREDENTIALS));
    try
    {
      $this->assertSame('test-project', (new BigQueryHelper($file))->gcpProjectName());
    }
    finally
    {
      unlink($file);
    }
  }

  public function invalidCredentialsProvider()
  {
    return [
      'null'               => [null, 'Invalid credentials provided'],
      'invalid json'       => ['{not json', 'The provided credentials are not in valid JSON format'],
      'missing project id' => [['type' => 'service_account'], 'Invalid credentials provided'],
      'directory'          => [__DIR__, 'The specified credentials file is not a file'],
    ];
  }

  /**
   * @dataProvider invalidCredentialsProvider
   */
  public function testInvalidCredentialsThrow($credentials, string $message)
  {
    $this->expectException(BigQueryException::class);
    $this->expectExceptionMessage($message);
    new BigQueryHelper($credentials);
  }

  public function testClientIsCreatedOnce()
  {
    $helper = $this->_helper();
    $client = $helper->getClient();
    $this->assertInstanceOf(BigQueryClient::class, $client);
    $this->assertSame($client, $helper->getClient());
  }

  public function testDataSetName()
  {
    $helper = $this->_helper();
    $this->assertSame('test_ds', $helper->getDataSetName());
    $this->assertSame($helper, $helper->setDataSetName('other'));
    $this->assertSame('other', $helper->getDataSetName());
  }

  public function testTrimDatasetFromTableName()
  {
    $helper = $this->_helper();
    $this->assertSame('events', $this->_call($helper, '_trimDatasetFromTableName', 'events'));
    $this->assertSame('events', $this->_call($helper, '_trimDatasetFromTableName', 'test_ds.events'));

    $this->expectException(BigQueryException::class);
    $this->expectExceptionMessage('Incorrect dataset in table name: other.events. Expected dataset test_ds');
    $this->_call($helper, '_trimDatasetFromTableName', 'other.events');
  }

  public function testSplitTableNameForTemplate()
  {
    $helper = $this->_helper();
    $this->assertSame(
      ['events_20260101', 'events', '_20260101'],
      $this->_call($helper, '_splitTableNameForTemplate', 'events_20260101')
    );

    $this->expectException(BigQueryException::class);
    $this->expectExceptionMessage('Table name not suitable for using template: events');
    $this->_call($helper, '_splitTableNameForTemplate', 'events');
  }

  public function testWriteablesToRowsSerialisesEveryType()
  {
    $schema = [
      ['name' => 'int', 'type' => BigQueryType::INTEGER],
      ['name' => 'ts', 'type' => BigQueryType::TIMESTAMP],
      ['name' => 'float', 'type' => BigQueryType::FLOAT],
      ['name' => 'bool', 'type' => BigQueryType::BOOLEAN],
      ['name' => 'str', 'type' => BigQueryType::STRING],
      ['name' => 'list', 'type' => BigQueryType::STRING],
      ['name' => 'bytes', 'type' => BigQueryType::BYTES],
      ['name' => 'missing', 'type' => BigQueryType::STRING],
      ['name' => 'rowTimestamp', 'type' => BigQueryType::TIMESTAMP],
    ];
    $data = ['int' => '5', 'ts' => 1700000000.9, 'float' => '1.5', 'bool' => 1, 'str' => 42, 'list' => ['a', 'b'],
             'bytes' => 'raw', 'rowTimestamp' => 'ignored'];

    $rows = $this->_call(
      $this->_helper(),
      '_writeablesToRows',
      ['tag' => $this->_writeable('events', 'id1', $data, $schema, 1700000001)]
    );

    $this->assertSame(
      [
        'tag' => [
          'insertId' => 'id1',
          'data'     => [
            'int'          => 5,
            'ts'           => 1700000000,
            'float'        => 1.5,
            'bool'         => true,
            'str'          => '42',
            'list'         => 'a, b',
            'bytes'        => 'cmF3',
            'missing'      => '',
            'rowTimestamp' => 1700000001,
          ],
        ],
      ],
      $rows
    );
  }

  public function testRowTimestampOmittedWhenEmpty()
  {
    $schema = [['name' => 'a', 'type' => BigQueryType::STRING], ['name' => 'rowTimestamp', 'type' => BigQueryType::TIMESTAMP]];
    $rows = $this->_call(
      $this->_helper(),
      '_writeablesToRows',
      [$this->_writeable('events', 'id1', ['a' => 'x'], $schema)]
    );
    $this->assertSame(['a' => 'x'], $rows[0]['data']);
  }

  public function invalidFieldProvider()
  {
    return [
      'non numeric integer' => [BigQueryType::INTEGER, 'abc', "Invalid data 'abc' for integer field `f`"],
      'array timestamp'     => [BigQueryType::TIMESTAMP, ['x'], "Invalid data 'array' for timestamp field `f`"],
      'non numeric float'   => [BigQueryType::FLOAT, '', "Invalid data '' for float field `f`"],
      'unknown type'        => [BigQueryType::RECORD, 'x', "Unable to serialize data for BigQuery: Unknown type 'record' for field `f`"],
    ];
  }

  /**
   * @dataProvider invalidFieldProvider
   */
  public function testInvalidFieldThrows(string $type, $value, string $message)
  {
    $this->expectException(BigQueryException::class);
    $this->expectExceptionMessage($message);
    $this->_call($this->_helper(), '_serializeField', $value, $type, 'f');
  }

  public function testGroupRowsByTableKeepsTags()
  {
    $schema = [['name' => 'a', 'type' => BigQueryType::STRING]];
    $first = $this->_writeable('one', 'i1', [], $schema);
    $second = $this->_writeable('two', 'i2', [], $schema);
    $third = $this->_writeable('one', 'i3', [], $schema);

    $this->assertSame(
      ['one' => ['x' => $first, 'z' => $third], 'two' => ['y' => $second]],
      $this->_call($this->_helper(), '_groupRowsByTable', ['x' => $first, 'y' => $second, 'z' => $third])
    );
  }

  public function testBatchRowsBySizeSplitsLargeRequests()
  {
    $helper = $this->_helper();
    $small = [['data' => 'a'], ['data' => 'b']];
    $this->assertSame([$small], $this->_call($helper, '_batchRowsBySize', $small));

    $row = ['data' => str_repeat('x', (int)(BigQueryHelper::MAX_REQUEST_DATA_SIZE * 0.4))];
    $batches = $this->_call($helper, '_batchRowsBySize', [$row, $row, $row, $row, $row]);
    $this->assertSame([2, 2, 1], array_map('count', $batches));
    foreach($batches as $batch)
    {
      $this->assertLessThanOrEqual(BigQueryHelper::MAX_REQUEST_DATA_SIZE, strlen(json_encode($batch)));
    }
  }

  public function testRowErrorsMessage()
  {
    $this->assertSame(
      'Index 3: [reason: invalid, location: a, message: bad a], [reason: stopped, location: b, message: bad b]',
      $this->_call(
        $this->_helper(),
        '_makeRowErrorsMsg',
        [
          'index'  => 3,
          'errors' => [
            ['reason' => 'invalid', 'location' => 'a', 'message' => 'bad a'],
            ['reason' => 'stopped', 'location' => 'b', 'message' => 'bad b'],
          ],
        ]
      )
    );
  }

  public function testWriteRowsWithNoItemsMakesNoRequests()
  {
    $this->assertSame([], $this->_helper()->writeRows([]));
  }
}

class TestWriteable implements IBigQueryWriteable
{
  private $_table;
  private $_insertId;
  private $_data;
  private $_schema;
  private $_rowTimestamp;

  public function __construct(string $table, string $insertId, array $data, array $schema, $rowTimestamp)
  {
    $this->_table = $table;
    $this->_insertId = $insertId;
    $this->_data = $data;
    $this->_schema = $schema;
    $this->_rowTimestamp = $rowTimestamp;
  }

  public function getBigQueryTableName()
  {
    return $this->_table;
  }

  public function getBigQuerySchema()
  {
    return $this->_schema;
  }

  public function getBigQueryData()
  {
    return $this->_data;
  }

  public function getBigQueryInsertId()
  {
    return $this->_insertId;
  }

  public function getBigQueryRowTimestamp()
  {
    return $this->_rowTimestamp;
  }
}
