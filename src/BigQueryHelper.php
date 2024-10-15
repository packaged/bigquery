<?php
namespace Packaged\BigQuery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Dataset;
use Google\Cloud\BigQuery\Job;
use Google\Cloud\BigQuery\QueryResults;
use Google\Cloud\Core\Exception\ConflictException;
use Google\Cloud\Core\Exception\NotFoundException;

class BigQueryHelper
{
  /** @var int Max size of the data to put in a BigQuery request when writing rows. Remember to allow some overhead. */
  const MAX_REQUEST_DATA_SIZE = 8388608; // 8MiB

  private $_credentials;
  /** @var BigQueryClient */
  private $_client = null;
  private $_dataSetName = null;
  private $_debugEnabled = false;

  // Internal caching
  /** @var array[] Caches table schemas */
  private $_tableSchemas = [];
  /** @var bool Prevents us from attempting to create the dataset multiple times */
  private $_dataSetExists = false;
  /** @var array Stops us doing multiple create table calls for the same table */
  private $_createdTables = [];
  /** @var Dataset Caches the dataset object */
  private $_dataSet = null;

  /**
   * BigQueryHelper constructor.
   *
   * @param string|array $credentials Path to a credentials file, JSON string of credentials file content, or array
   *                                  containing the decoded JSON
   * @param string|null  $dataSet
   * @param bool         $debugEnabled
   *
   * @throws BigQueryException
   */
  public function __construct($credentials = null, $dataSet = null, $debugEnabled = false)
  {
    $this->_credentials = $this->_loadCredentials($credentials);
    $this->_dataSetName = $dataSet;
    $this->_debugEnabled = $debugEnabled;
  }

  protected function _debug($msg)
  {
    if($this->_debugEnabled)
    {
      $this->_log($msg);
    }
  }

  protected function _log($msg)
  {
    error_log('BigQueryHelper: ' . $msg);
  }

  public function setDebugEnabled($debugEnabled)
  {
    $this->_debugEnabled = $debugEnabled;
    return $this;
  }

  public function isDebugEnabled()
  {
    return $this->_debugEnabled;
  }

  public function gcpProjectName()
  {
    return $this->_credentials['project_id'];
  }

  /**
   * @param string|array $credentials
   *
   * @return array
   * @throws BigQueryException
   */
  private function _loadCredentials($credentials)
  {
    // Load/decode credentials
    if(is_string($credentials))
    {
      if(file_exists($credentials))
      {
        if(!is_file($credentials))
        {
          throw new BigQueryException('The specified credentials file is not a file');
        }
        $credentials = file_get_contents($credentials);
      }

      $decoded = json_decode($credentials, true);
      if(!$decoded)
      {
        throw new BigQueryException('The provided credentials are not in valid JSON format');
      }
      $credentials = $decoded;
    }
    if((!is_array($credentials)) || (empty($credentials['project_id'])))
    {
      throw new BigQueryException(('Invalid credentials provided'));
    }
    return $credentials;
  }

  /**
   * @return BigQueryClient
   */
  public function getClient()
  {
    if($this->_client === null)
    {
      $this->_client = new BigQueryClient(['keyFile' => $this->_credentials]);
    }
    return $this->_client;
  }

  public function setDataSetName($name)
  {
    if($name != $this->_dataSetName)
    {
      $this->_dataSetName = $name;
      $this->_dataSet = null;
    }
    return $this;
  }

  public function getDataSetName()
  {
    return $this->_dataSetName;
  }

  public function getDataSet()
  {
    if(!$this->_dataSet)
    {
      $this->_dataSet = $this->getClient()->dataset($this->getDataSetName());
      if(!$this->_dataSetExists)
      {
        $this->_dataSetExists = $this->_dataSet->exists();
      }
    }
    return $this->_dataSet;
  }

  /**
   * Create the dataset for the current organisation
   *
   * @param string $description
   *
   * @throws ConflictException
   */
  public function createDataset($description = null)
  {
    if(!$this->_dataSetExists)
    {
      $dataSetName = $this->getDataSetName();
      $this->_log("Creating dataset " . $dataSetName);
      try
      {
        $this->getClient()->createDataset($dataSetName, ['description' => $description]);
      }
      catch(ConflictException $e)
      {
        // ConflictException with code 409 is "already exists" so only throw if that's not the code
        if($e->getCode() != 409)
        {
          throw $e;
        }
      }
      $this->_dataSetExists = true;
    }
  }

  /**
   * Create a new table for a given IBigQueryWriteable object
   *
   * @param IBigQueryWriteable $object
   * @param bool               $useTemplateTable If true then use template tables to write the data
   *
   * @throws BigQueryException
   * @throws ConflictException
   */
  public function createTableForObject(IBigQueryWriteable $object, $useTemplateTable = false)
  {
    $tableName = $object->getBigQueryTableName();
    if($useTemplateTable)
    {
      list(, $tableName,) = $this->_splitTableNameForTemplate($tableName);
    }

    $this->createTable(
      $tableName,
      get_class($object),
      $object->getBigQuerySchema()
    );
  }

  /**
   * Create a new table
   *
   * @param string $tableName
   * @param string $description
   * @param array  $fields Array of fields where each field is in this format:
   *                       ['name' => 'fieldName', 'type' => 'integer', 'mode' => 'required']
   *
   * @throws BigQueryException
   * @throws ConflictException
   */
  public function createTable($tableName, $description, $fields)
  {
    $tableName = $this->_trimDatasetFromTableName($tableName);
    if(!isset($this->_createdTables[$tableName]))
    {
      $this->_log("Creating table " . $this->getDataSetName() . '.' . $tableName);

      try
      {
        $this->getDataSet()->createTable(
          $tableName,
          ['description' => $description, 'schema' => ['fields' => $fields]]
        );
      }
      catch(ConflictException $e)
      {
        // ConflictException with code 409 is "already exists" so only throw if that's not the code
        if($e->getCode() != 409)
        {
          throw $e;
        }
      }
    }
    $this->_createdTables[$tableName] = true;
  }

  /**
   * @param string $tableName
   * @param bool   $noCache
   *
   * @return array
   * @throws BigQueryException
   * @throws NotFoundException
   */
  public function getTableSchema($tableName, $noCache = false)
  {
    if($noCache || (!isset($this->_tableSchemas[$tableName])))
    {
      $tableName = $this->_trimDatasetFromTableName($tableName);
      $info = $this->getDataSet()->table($tableName)->info();
      $this->_tableSchemas[$tableName] = $info['schema'];
    }
    return $this->_tableSchemas[$tableName];
  }

  /**
   * @param string $tableName
   * @param bool   $detailed if true then include name, type and mode. Otherwise just name.
   * @param bool   $noCache
   *
   * @return string[]|array[]
   * @throws BigQueryException
   * @throws NotFoundException
   */
  public function getFieldsInTable($tableName, $detailed = false, $noCache = false)
  {
    $schema = $this->getTableSchema($tableName, $noCache);
    if($detailed)
    {
      return $schema['fields'];
    }
    $names = [];
    foreach($schema['fields'] as $idx => $field)
    {
      if(!isset($field['name']))
      {
        throw new BigQueryException('field at index ' . $idx . ' has no name: ' . print_r($field, true));
      }
      $names[] = $field['name'];
    }
    return $names;
  }

  /**
   * Get a list of the tables in the current dataset
   *
   * @return string[]
   */
  public function listTables()
  {
    $tableNames = [];
    $this->iterateTables(
      function ($tableName) use (&$tableNames) {
        $tableNames[] = $tableName;
      }
    );
    return $tableNames;
  }

  /**
   * Perform a callback for all tables in the dataset. The callback is called with the table name as its only argument.
   *
   * @param callable $callback
   */
  public function iterateTables(callable $callback)
  {
    $opts = ['maxResults' => 100]; // max 100 results per page
    $tables = $this->getDataSet()->tables($opts);
    foreach($tables->iterateByPage() as $page)
    {
      /** @var \Google\Cloud\BigQuery\Table $table */
      foreach($page as $table)
      {
        $callback($table->id());
      }
    }
  }

  /**
   * @param string $sql
   * @param bool   $async
   * @param bool   $legacySql
   * @param array  $extraQueryOpts An array of options to add to the configuration.query part of the request config
   *
   * @return Job|QueryResults Returns a Job if $async is true, otherwise returns QueryResults
   * @throws BigQueryTimeoutException
   */
  public function runQuery($sql, $async = false, $legacySql = true, $extraQueryOpts = [])
  {
    return $this->_runQuery($sql, $async, $legacySql, $extraQueryOpts);
  }

  /**
   * @param QueryResults $result
   *
   * @return array[] Array of rows, each as a key->value array
   * @throws \Google\Cloud\BigQuery\JobException
   * @throws \Google\Cloud\Core\Exception\GoogleException
   */
  public function dataFromResult(QueryResults $result)
  {
    $rows = [];
    foreach($result->rows() as $row)
    {
      $rows[] = $row;
    }
    return $rows;
  }

  /**
   * Run a query and place the results in the specified table. The destination table will be created if it does not
   * exist, otherwise it will be truncated before writing
   *
   * @param string $sql
   * @param string $destTable
   * @param bool   $async
   * @param bool   $legacySql
   * @param bool   $truncateTable
   *
   * @return Job|QueryResults Returns a Job if $async is true, otherwise returns QueryResults
   * @throws BigQueryException
   */
  public function runQueryIntoTable($sql, $destTable, $async = false, $legacySql = true, $truncateTable = true)
  {
    $destTable = $this->_trimDatasetFromTableName($destTable);
    return $this->_runQuery(
      $sql,
      $async,
      $legacySql,
      [
        'destinationTable'  => [
          'projectId' => $this->gcpProjectName(),
          'datasetId' => $this->getDataSetName(),
          'tableId'   => $destTable,
        ],
        'createDisposition' => 'CREATE_IF_NEEDED',
        'writeDisposition'  => $truncateTable ? 'WRITE_TRUNCATE' : 'WRITE_APPEND',
        'allowLargeResults' => true,
      ],
      ['maxResults' => 1]
    );
  }

  /**
   * @param string $sql
   * @param bool   $async
   * @param bool   $legacySql
   * @param array  $extraOpts           An array of options to add to the configuration.query part of the request config
   * @param array  $queryResultsOptions Options to pass in when retrieving the results in synchronous queries
   *
   * @return Job|QueryResults Returns a job if $async is true, otherwise returns QueryResults
   * @throws BigQueryTimeoutException
   */
  private function _runQuery($sql, $async = false, $legacySql = true, $extraOpts = [], $queryResultsOptions = [])
  {
    $client = $this->getClient();
    $jobConfig = $client->query($sql, ['configuration' => ['query' => $extraOpts]]);
    $jobConfig->useLegacySql($legacySql);
    if($async)
    {
      return $client->startQuery($jobConfig);
    }
    $result = $client->runQuery($jobConfig, $queryResultsOptions);

    // Check whether the query has completed
    $info = $result->info();
    if($info && empty($info['jobComplete']))
    {
      throw new BigQueryTimeoutException(
        "Timed out retrieving BigQuery data",
        0,
        null,
        $sql,
        isset($info["jobReference"]["projectId"]) ? $info["jobReference"]["projectId"] : '',
        isset($info["jobReference"]["jobId"]) ? $info["jobReference"]["jobId"] : '',
        isset($info["jobReference"]["location"]) ? $info["jobReference"]["location"] : ''
      );
    }
    return $result;
  }

  /**
   * Remove dataset from beginning of table if it has one
   *
   * @param string $tableName
   *
   * @return string
   * @throws BigQueryException
   */
  private function _trimDatasetFromTableName($tableName)
  {
    if(strpos($tableName, '.') !== false)
    {
      $parts = explode('.', $tableName, 2);
      if($parts[0] != $this->getDataSetName())
      {
        throw new BigQueryException(
          'Incorrect dataset in table name: ' . $tableName
          . '. Expected dataset ' . $this->getDataSetName()
        );
      }
      $tableName = $parts[1];
    }
    return $tableName;
  }

  /**
   * @param IBigQueryWriteable[] $rows
   *
   * @return array
   */
  protected function _groupRowsByTable(array $rows)
  {
    $grouped = [];
    foreach($rows as $key => $row)
    {
      $tableName = $row->getBigQueryTableName();
      if(isset($grouped[$tableName]))
      {
        $grouped[$tableName][$key] = $row;
      }
      else
      {
        $grouped[$tableName] = [$key => $row];
      }
    }
    return $grouped;
  }

  /**
   * @param IBigQueryWriteable[] $items              An array of items to write to BigQuery. This should be indexed with
   *                                                 unique tags (numbers should also work).
   * @param bool                 $autoCreateTables   If this is true then datasets and tables will be auto-created if
   *                                                 they do not exist. If this happens the writes will be NOT retried
   *                                                 after creating the tables.
   * @param bool                 $useTemplatedTables If true then use templated tables to perform the write
   * @param callable|null        $onError            Function to call for each set of errors. Called with arguments
   *                                                 ($tableName, $tags, $error)
   * @param callable|null        $onSuccess          Function to call for each set of successfil writes. Called with
   *                                                 arguments ($tableName, $tags)
   *
   * @return string[] Errors encountered during the operation, indexed by table name
   * @throws BigQueryException
   * @throws ConflictException
   */
  public function writeRows(
    array    $items, $useTemplatedTables = false, $autoCreateTables = false, callable $onError = null,
    callable $onSuccess = null
  )
  {
    $groupedItems = $this->_groupRowsByTable($items);
    if(count($groupedItems) < 1)
    {
      return [];
    }

    $startTime = microtime(true);
    $totalRows = count($items);
    $errorCount = 0;

    $dataSet = $this->getDataSet();
    $this->createDataset();

    // Make a map of insert IDs to message tags
    $idTagMap = [];
    foreach($items as $tag => $item)
    {
      $idTagMap[$item->getBigQueryInsertId()] = $tag;
    }

    // Build the batch of requests to send to BigQuery
    $errors = [];
    /** @var IBigQueryWriteable[] $rows */
    foreach($groupedItems as $fullTableName => $rows)
    {
      if(count($rows) < 1)
      {
        continue;
      }
      /** @var IBigQueryWriteable $firstRow */
      $firstRow = reset($rows);

      $options = ['autoCreate' => $autoCreateTables];
      if($useTemplatedTables)
      {
        list(, $targetTable, $suffix) = $this->_splitTableNameForTemplate($fullTableName);
        $options ['templateSuffix'] = $suffix;
      }
      else
      {
        $targetTable = $fullTableName;
      }

      if($autoCreateTables)
      {
        $options['tableMetadata'] = ['schema' => ['fields' => $firstRow->getBigQuerySchema()]];
      }

      $failedTagsKeyed = [];
      $successTagsKeyed = array_fill_keys(array_keys($rows), true);

      foreach($this->_batchRowsBySize($rows) as $rowBatch)
      {
        $bqRows = $this->_writeablesToRows($rowBatch);
        $result = $dataSet->table($targetTable)->insertRows($bqRows, $options);

        if(!$result->isSuccessful())
        {
          $failedRows = $result->failedRows();
          $rowsByIdx = array_values($rowBatch);
          foreach($failedRows as $row)
          {
            $idx = $row['index'];
            if(isset($rowsByIdx[$idx]))
            {
              $tag = $idTagMap[$rowsByIdx[$idx]->getBigQueryInsertId()];
              $failedTagsKeyed[$tag] = true;
              unset($successTagsKeyed[$tag]);
            }

            // Make the errors from all rows into a single string
            if(!empty($row['errors']))
            {
              $msg = $this->_makeRowErrorsMsg($row);
              $this->_debug(
                'Errors inserting into table ' . $this->getDataSetName() . '.' . $fullTableName . ': ' . $msg
              );
              $errors[$fullTableName] = $msg;
            }

            $errorCount++;
          }
        }
      }

      if($onError && (count($failedTagsKeyed) > 0))
      {
        $onError(
          $fullTableName,
          array_keys($failedTagsKeyed),
          isset($errors[$fullTableName]) ? $errors[$fullTableName] : 'Unknown error'
        );
      }
      if($onSuccess && (count($successTagsKeyed) > 0))
      {
        $onSuccess($fullTableName, array_keys($successTagsKeyed));
      }
    }

    $duration = microtime(true) - $startTime;

    $this->_debug(
      'Wrote ' . ($totalRows - $errorCount) . '/' . $totalRows . ' rows in ' . round($duration * 1000) . 'ms'
    );

    return $errors;
  }

  /**
   * Group an array of BigQuery rows into batches where the full size of each batch does not exceed
   * MAX_REQUEST_DATA_SIZE
   *
   * @param array $rows
   *
   * @return array[]
   */
  private function _batchRowsBySize(array $rows)
  {
    if(strlen(json_encode($rows)) <= self::MAX_REQUEST_DATA_SIZE)
    {
      return [$rows];
    }

    $groupedRows = [];
    $batchSize = 0;
    $thisBatch = [];
    foreach($rows as $row)
    {
      $rowSize = strlen(json_encode($row));

      if(($batchSize + $rowSize) > self::MAX_REQUEST_DATA_SIZE)
      {
        // Add this batch to the output and start a new batch
        if(count($thisBatch) > 0)
        {
          $groupedRows[] = $thisBatch;
        }
        $batchSize = 0;
        $thisBatch = [];
      }

      $thisBatch[] = $row;
      $batchSize += $rowSize;
    }
    // Add the last batch to the output
    if(count($thisBatch) > 0)
    {
      $groupedRows[] = $thisBatch;
    }

    return $groupedRows;
  }

  /**
   * Drop a table
   *
   * @param string $tableName
   *
   * @throws NotFoundException
   */
  public function dropTable($tableName)
  {
    $dataSet = $this->getDataSet();
    try
    {
      $this->_log("Dropping table " . $tableName);
      $dataSet->table($tableName)->delete();
    }
    catch(NotFoundException $e)
    {
      if($e->getCode() != 404)
      {
        throw $e;
      }
    }
  }

  /**
   * Split a table name into its template table and suffix
   *
   * @param string $tableName
   *
   * @return string[]
   * @throws BigQueryException
   */
  protected function _splitTableNameForTemplate($tableName)
  {
    preg_match('/^(.*)(_[0-9]{8})$/', $tableName, $matches);
    if(count($matches) != 3)
    {
      throw new BigQueryException(
        'Table name not suitable for using template: ' . $tableName
      );
    }
    // [fullName, prefix, suffix]
    return [$tableName, $matches[1], $matches[2]];
  }

  /**
   * @param IBigQueryWriteable[] $writeables
   *
   * @return array An array in an appropriate format to pass to Table::insertRows
   * @throws BigQueryException
   */
  protected function _writeablesToRows(array $writeables)
  {
    $rows = [];
    foreach($writeables as $key => $writeable)
    {
      $rows[$key] = [
        'insertId' => $writeable->getBigQueryInsertId(),
        'data'     => $this->_serializeRow($writeable),
      ];
    }
    return $rows;
  }

  /**
   * @param IBigQueryWriteable $row
   *
   * @return array
   * @throws BigQueryException
   */
  protected function _serializeRow(IBigQueryWriteable $row)
  {
    $bqData = [];
    $rowData = $row->getBigQueryData();
    $schema = $row->getBigQuerySchema();
    $hasRowTimestamp = false;
    foreach($schema as $field)
    {
      $fieldName = $field['name'];
      if($fieldName == 'rowTimestamp')
      {
        $hasRowTimestamp = true;
      }
      else
      {
        $value = isset($rowData[$fieldName]) ? $rowData[$fieldName] : '';
        $bqData[$fieldName] = $this->_serializeField($value, $field['type'], $field['name']);
      }
    }

    if($hasRowTimestamp)
    {
      $rowTimestamp = $row->getBigQueryRowTimestamp();
      if($rowTimestamp)
      {
        $bqData['rowTimestamp'] = $rowTimestamp;
      }
    }
    return $bqData;
  }

  /**
   * @param mixed  $value
   * @param string $fieldType
   * @param string $fieldName
   *
   * @return bool|float|int|string
   * @throws BigQueryException
   */
  protected function _serializeField($value, $fieldType, $fieldName)
  {
    $errMsg = "Invalid data '" . (is_scalar($value) ? $value : gettype($value))
      . "' for " . $fieldType . " field `" . $fieldName . "`";
    switch($fieldType)
    {
      case BigQueryType::INTEGER:
      case BigQueryType::TIMESTAMP:
        if(!is_numeric($value))
        {
          throw new BigQueryException($errMsg);
        }
        $tidyData = (int)$value;
        break;
      case BigQueryType::FLOAT:
        if(!is_numeric($value))
        {
          throw new BigQueryException($errMsg);
        }
        $tidyData = (float)$value;
        break;
      case BigQueryType::BOOLEAN:
        $tidyData = (bool)$value;
        break;
      case BigQueryType::STRING:
        $tidyData = is_array($value) ? implode(', ', $value) : (string)$value;
        break;
      case BigQueryType::BYTES:
        $tidyData = base64_encode($value);
        break;
      default:
        throw new BigQueryException(
          "Unable to serialize data for BigQuery: Unknown type '" . $fieldType . "' for field `" . $fieldName . "`"
        );
    }
    return $tidyData;
  }

  /**
   * @param array $rowError
   *
   * @return string
   */
  private function _makeRowErrorsMsg(array $rowError)
  {
    $messages = [];
    foreach($rowError['errors'] as $error)
    {
      $messages[] = sprintf(
        '[reason: %s, location: %s, message: %s]',
        $error['reason'],
        $error['location'],
        $error['message']
      );
    }

    return sprintf("Index %d: %s", $rowError['index'], implode(', ', $messages));
  }
}
